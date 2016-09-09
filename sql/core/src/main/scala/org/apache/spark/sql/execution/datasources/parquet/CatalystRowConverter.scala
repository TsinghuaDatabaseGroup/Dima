/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import java.math.{BigDecimal, BigInteger}
import java.nio.ByteOrder

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.OriginalType.{INT_32, LIST, UTF8}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{DOUBLE, INT32, INT64, BINARY, FIXED_LEN_BYTE_ARRAY}
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayBasedMapData, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A [[ParentContainerUpdater]] is used by a Parquet converter to set converted values to some
 * corresponding parent container. For example, a converter for a `StructType` field may set
 * converted values to a [[MutableRow]]; or a converter for array elements may append converted
 * values to an [[ArrayBuffer]].
 */
private[parquet] trait ParentContainerUpdater {
  /** Called before a record field is being converted */
  def start(): Unit = ()

  /** Called after a record field is being converted */
  def end(): Unit = ()

  def set(value: Any): Unit = ()
  def setBoolean(value: Boolean): Unit = set(value)
  def setByte(value: Byte): Unit = set(value)
  def setShort(value: Short): Unit = set(value)
  def setInt(value: Int): Unit = set(value)
  def setLong(value: Long): Unit = set(value)
  def setFloat(value: Float): Unit = set(value)
  def setDouble(value: Double): Unit = set(value)
}

/** A no-op updater used for root converter (who doesn't have a parent). */
private[parquet] object NoopUpdater extends ParentContainerUpdater

private[parquet] trait HasParentContainerUpdater {
  def updater: ParentContainerUpdater
}

/**
 * A convenient converter class for Parquet group types with an [[HasParentContainerUpdater]].
 */
private[parquet] abstract class CatalystGroupConverter(val updater: ParentContainerUpdater)
  extends GroupConverter with HasParentContainerUpdater

/**
 * Parquet converter for Parquet primitive types.  Note that not all Spark SQL atomic types
 * are handled by this converter.  Parquet primitive types are only a subset of those of Spark
 * SQL.  For example, BYTE, SHORT, and INT in Spark SQL are all covered by INT32 in Parquet.
 */
private[parquet] class CatalystPrimitiveConverter(val updater: ParentContainerUpdater)
  extends PrimitiveConverter with HasParentContainerUpdater {

  override def addBoolean(value: Boolean): Unit = updater.setBoolean(value)
  override def addInt(value: Int): Unit = updater.setInt(value)
  override def addLong(value: Long): Unit = updater.setLong(value)
  override def addFloat(value: Float): Unit = updater.setFloat(value)
  override def addDouble(value: Double): Unit = updater.setDouble(value)
  override def addBinary(value: Binary): Unit = updater.set(value.getBytes)
}

/**
 * A [[CatalystRowConverter]] is used to convert Parquet records into Catalyst [[InternalRow]]s.
 * Since Catalyst `StructType` is also a Parquet record, this converter can be used as root
 * converter.  Take the following Parquet type as an example:
 * {{{
 *   message root {
 *     required int32 f1;
 *     optional group f2 {
 *       required double f21;
 *       optional binary f22 (utf8);
 *     }
 *   }
 * }}}
 * 5 converters will be created:
 *
 * - a root [[CatalystRowConverter]] for [[MessageType]] `root`, which contains:
 *   - a [[CatalystPrimitiveConverter]] for required [[INT_32]] field `f1`, and
 *   - a nested [[CatalystRowConverter]] for optional [[GroupType]] `f2`, which contains:
 *     - a [[CatalystPrimitiveConverter]] for required [[DOUBLE]] field `f21`, and
 *     - a [[CatalystStringConverter]] for optional [[UTF8]] string field `f22`
 *
 * When used as a root converter, [[NoopUpdater]] should be used since root converters don't have
 * any "parent" container.
 *
 * @param parquetType Parquet schema of Parquet records
 * @param catalystType Spark SQL schema that corresponds to the Parquet record type. User-defined
 *        types should have been expanded.
 * @param updater An updater which propagates converted field values to the parent container
 */
private[parquet] class CatalystRowConverter(
    parquetType: GroupType,
    catalystType: StructType,
    updater: ParentContainerUpdater)
  extends CatalystGroupConverter(updater) with Logging {

  assert(
    parquetType.getFieldCount == catalystType.length,
    s"""Field counts of the Parquet schema and the Catalyst schema don't match:
       |
       |Parquet schema:
       |$parquetType
       |Catalyst schema:
       |${catalystType.prettyJson}
     """.stripMargin)

  assert(
    !catalystType.existsRecursively(_.isInstanceOf[UserDefinedType[_]]),
    s"""User-defined types in Catalyst schema should have already been expanded:
       |${catalystType.prettyJson}
     """.stripMargin)

  logDebug(
    s"""Building row converter for the following schema:
       |
       |Parquet form:
       |$parquetType
       |Catalyst form:
       |${catalystType.prettyJson}
     """.stripMargin)

  /**
   * Updater used together with field converters within a [[CatalystRowConverter]].  It propagates
   * converted filed values to the `ordinal`-th cell in `currentRow`.
   */
  private final class RowUpdater(row: MutableRow, ordinal: Int) extends ParentContainerUpdater {
    override def set(value: Any): Unit = row(ordinal) = value
    override def setBoolean(value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(value: Float): Unit = row.setFloat(ordinal, value)
  }

  private val currentRow = new SpecificMutableRow(catalystType.map(_.dataType))

  private val unsafeProjection = UnsafeProjection.create(catalystType)

  /**
   * The [[UnsafeRow]] converted from an entire Parquet record.
   */
  def currentRecord: UnsafeRow = unsafeProjection(currentRow)

  // Converters for each field.
  private val fieldConverters: Array[Converter with HasParentContainerUpdater] = {
    parquetType.getFields.asScala.zip(catalystType).zipWithIndex.map {
      case ((parquetFieldType, catalystField), ordinal) =>
        // Converted field value should be set to the `ordinal`-th cell of `currentRow`
        newConverter(parquetFieldType, catalystField.dataType, new RowUpdater(currentRow, ordinal))
    }.toArray
  }

  override def getConverter(fieldIndex: Int): Converter = fieldConverters(fieldIndex)

  override def end(): Unit = {
    var i = 0
    while (i < currentRow.numFields) {
      fieldConverters(i).updater.end()
      i += 1
    }
    updater.set(currentRow)
  }

  override def start(): Unit = {
    var i = 0
    while (i < currentRow.numFields) {
      fieldConverters(i).updater.start()
      currentRow.setNullAt(i)
      i += 1
    }
  }

  /**
   * Creates a converter for the given Parquet type `parquetType` and Spark SQL data type
   * `catalystType`. Converted values are handled by `updater`.
   */
  private def newConverter(
      parquetType: Type,
      catalystType: DataType,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater = {

    catalystType match {
      case BooleanType | IntegerType | LongType | FloatType | DoubleType | BinaryType =>
        new CatalystPrimitiveConverter(updater)

      case ByteType =>
        new CatalystPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit =
            updater.setByte(value.asInstanceOf[ByteType#InternalType])
        }

      case ShortType =>
        new CatalystPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit =
            updater.setShort(value.asInstanceOf[ShortType#InternalType])
        }

      // For INT32 backed decimals
      case t: DecimalType if parquetType.asPrimitiveType().getPrimitiveTypeName == INT32 =>
        new CatalystIntDictionaryAwareDecimalConverter(t.precision, t.scale, updater)

      // For INT64 backed decimals
      case t: DecimalType if parquetType.asPrimitiveType().getPrimitiveTypeName == INT64 =>
        new CatalystLongDictionaryAwareDecimalConverter(t.precision, t.scale, updater)

      // For BINARY and FIXED_LEN_BYTE_ARRAY backed decimals
      case t: DecimalType
        if parquetType.asPrimitiveType().getPrimitiveTypeName == FIXED_LEN_BYTE_ARRAY ||
           parquetType.asPrimitiveType().getPrimitiveTypeName == BINARY =>
        new CatalystBinaryDictionaryAwareDecimalConverter(t.precision, t.scale, updater)

      case t: DecimalType =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for decimal type ${t.json} whose Parquet type is " +
            s"$parquetType.  Parquet DECIMAL type can only be backed by INT32, INT64, " +
            "FIXED_LEN_BYTE_ARRAY, or BINARY.")

      case StringType =>
        new CatalystStringConverter(updater)

      case TimestampType =>
        // TODO Implements `TIMESTAMP_MICROS` once parquet-mr has that.
        new CatalystPrimitiveConverter(updater) {
          // Converts nanosecond timestamps stored as INT96
          override def addBinary(value: Binary): Unit = {
            assert(
              value.length() == 12,
              "Timestamps (with nanoseconds) are expected to be stored in 12-byte long binaries, " +
              s"but got a ${value.length()}-byte binary.")

            val buf = value.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN)
            val timeOfDayNanos = buf.getLong
            val julianDay = buf.getInt
            updater.setLong(DateTimeUtils.fromJulianDay(julianDay, timeOfDayNanos))
          }
        }

      case DateType =>
        new CatalystPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit = {
            // DateType is not specialized in `SpecificMutableRow`, have to box it here.
            updater.set(value.asInstanceOf[DateType#InternalType])
          }
        }

      // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
      // annotated by `LIST` or `MAP` should be interpreted as a required list of required
      // elements where the element type is the type of the field.
      case t: ArrayType if parquetType.getOriginalType != LIST =>
        if (parquetType.isPrimitive) {
          new RepeatedPrimitiveConverter(parquetType, t.elementType, updater)
        } else {
          new RepeatedGroupConverter(parquetType, t.elementType, updater)
        }

      case t: ArrayType =>
        new CatalystArrayConverter(parquetType.asGroupType(), t, updater)

      case t: MapType =>
        new CatalystMapConverter(parquetType.asGroupType(), t, updater)

      case t: StructType =>
        new CatalystRowConverter(parquetType.asGroupType(), t, new ParentContainerUpdater {
          override def set(value: Any): Unit = updater.set(value.asInstanceOf[InternalRow].copy())
        })

      case t =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for data type ${t.json} " +
            s"whose Parquet type is $parquetType")
    }
  }

  /**
   * Parquet converter for strings. A dictionary is used to minimize string decoding cost.
   */
  private final class CatalystStringConverter(updater: ParentContainerUpdater)
    extends CatalystPrimitiveConverter(updater) {

    private var expandedDictionary: Array[UTF8String] = null

    override def hasDictionarySupport: Boolean = true

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { i =>
        UTF8String.fromBytes(dictionary.decodeToBinary(i).getBytes)
      }
    }

    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    override def addBinary(value: Binary): Unit = {
      // The underlying `ByteBuffer` implementation is guaranteed to be `HeapByteBuffer`, so here we
      // are using `Binary.toByteBuffer.array()` to steal the underlying byte array without copying
      // it.
      val buffer = value.toByteBuffer
      val offset = buffer.position()
      val numBytes = buffer.limit() - buffer.position()
      updater.set(UTF8String.fromBytes(buffer.array(), offset, numBytes))
    }
  }

  /**
   * Parquet converter for fixed-precision decimals.
   */
  private abstract class CatalystDecimalConverter(
      precision: Int, scale: Int, updater: ParentContainerUpdater)
    extends CatalystPrimitiveConverter(updater) {

    protected var expandedDictionary: Array[Decimal] = _

    override def hasDictionarySupport: Boolean = true

    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    // Converts decimals stored as INT32
    override def addInt(value: Int): Unit = {
      addLong(value: Long)
    }

    // Converts decimals stored as INT64
    override def addLong(value: Long): Unit = {
      updater.set(decimalFromLong(value))
    }

    // Converts decimals stored as either FIXED_LENGTH_BYTE_ARRAY or BINARY
    override def addBinary(value: Binary): Unit = {
      updater.set(decimalFromBinary(value))
    }

    protected def decimalFromLong(value: Long): Decimal = {
      Decimal(value, precision, scale)
    }

    protected def decimalFromBinary(value: Binary): Decimal = {
      if (precision <= CatalystSchemaConverter.MAX_PRECISION_FOR_INT64) {
        // Constructs a `Decimal` with an unscaled `Long` value if possible.
        val unscaled = CatalystRowConverter.binaryToUnscaledLong(value)
        Decimal(unscaled, precision, scale)
      } else {
        // Otherwise, resorts to an unscaled `BigInteger` instead.
        Decimal(new BigDecimal(new BigInteger(value.getBytes), scale), precision, scale)
      }
    }
  }

  private class CatalystIntDictionaryAwareDecimalConverter(
      precision: Int, scale: Int, updater: ParentContainerUpdater)
    extends CatalystDecimalConverter(precision, scale, updater) {

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { id =>
        decimalFromLong(dictionary.decodeToInt(id).toLong)
      }
    }
  }

  private class CatalystLongDictionaryAwareDecimalConverter(
      precision: Int, scale: Int, updater: ParentContainerUpdater)
    extends CatalystDecimalConverter(precision, scale, updater) {

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { id =>
        decimalFromLong(dictionary.decodeToLong(id))
      }
    }
  }

  private class CatalystBinaryDictionaryAwareDecimalConverter(
      precision: Int, scale: Int, updater: ParentContainerUpdater)
    extends CatalystDecimalConverter(precision, scale, updater) {

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { id =>
        decimalFromBinary(dictionary.decodeToBinary(id))
      }
    }
  }

  /**
   * Parquet converter for arrays.  Spark SQL arrays are represented as Parquet lists.  Standard
   * Parquet lists are represented as a 3-level group annotated by `LIST`:
   * {{{
   *   <list-repetition> group <name> (LIST) {            <-- parquetSchema points here
   *     repeated group list {
   *       <element-repetition> <element-type> element;
   *     }
   *   }
   * }}}
   * The `parquetSchema` constructor argument points to the outermost group.
   *
   * However, before this representation is standardized, some Parquet libraries/tools also use some
   * non-standard formats to represent list-like structures.  Backwards-compatibility rules for
   * handling these cases are described in Parquet format spec.
   *
   * @see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
   */
  private final class CatalystArrayConverter(
      parquetSchema: GroupType,
      catalystSchema: ArrayType,
      updater: ParentContainerUpdater)
    extends CatalystGroupConverter(updater) {

    private var currentArray: ArrayBuffer[Any] = _

    private val elementConverter: Converter = {
      val repeatedType = parquetSchema.getType(0)
      val elementType = catalystSchema.elementType
      val parentName = parquetSchema.getName

      if (isElementType(repeatedType, elementType, parentName)) {
        newConverter(repeatedType, elementType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentArray += value
        })
      } else {
        new ElementConverter(repeatedType.asGroupType().getType(0), elementType)
      }
    }

    override def getConverter(fieldIndex: Int): Converter = elementConverter

    override def end(): Unit = updater.set(new GenericArrayData(currentArray.toArray))

    // NOTE: We can't reuse the mutable `ArrayBuffer` here and must instantiate a new buffer for the
    // next value.  `Row.copy()` only copies row cells, it doesn't do deep copy to objects stored
    // in row cells.
    override def start(): Unit = currentArray = ArrayBuffer.empty[Any]

    // scalastyle:off
    /**
     * Returns whether the given type is the element type of a list or is a syntactic group with
     * one field that is the element type.  This is determined by checking whether the type can be
     * a syntactic group and by checking whether a potential syntactic group matches the expected
     * schema.
     * {{{
     *   <list-repetition> group <name> (LIST) {
     *     repeated group list {                          <-- repeatedType points here
     *       <element-repetition> <element-type> element;
     *     }
     *   }
     * }}}
     * In short, here we handle Parquet list backwards-compatibility rules on the read path.  This
     * method is based on `AvroIndexedRecordConverter.isElementType`.
     *
     * @see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
     */
    // scalastyle:on
    private def isElementType(
        parquetRepeatedType: Type, catalystElementType: DataType, parentName: String): Boolean = {
      (parquetRepeatedType, catalystElementType) match {
        case (t: PrimitiveType, _) => true
        case (t: GroupType, _) if t.getFieldCount > 1 => true
        case (t: GroupType, _) if t.getFieldCount == 1 && t.getName == "array" => true
        case (t: GroupType, _) if t.getFieldCount == 1 && t.getName == parentName + "_tuple" => true
        case (t: GroupType, StructType(Array(f))) if f.name == t.getFieldName(0) => true
        case _ => false
      }
    }

    /** Array element converter */
    private final class ElementConverter(parquetType: Type, catalystType: DataType)
      extends GroupConverter {

      private var currentElement: Any = _

      private val converter = newConverter(parquetType, catalystType, new ParentContainerUpdater {
        override def set(value: Any): Unit = currentElement = value
      })

      override def getConverter(fieldIndex: Int): Converter = converter

      override def end(): Unit = currentArray += currentElement

      override def start(): Unit = currentElement = null
    }
  }

  /** Parquet converter for maps */
  private final class CatalystMapConverter(
      parquetType: GroupType,
      catalystType: MapType,
      updater: ParentContainerUpdater)
    extends CatalystGroupConverter(updater) {

    private var currentKeys: ArrayBuffer[Any] = _
    private var currentValues: ArrayBuffer[Any] = _

    private val keyValueConverter = {
      val repeatedType = parquetType.getType(0).asGroupType()
      new KeyValueConverter(
        repeatedType.getType(0),
        repeatedType.getType(1),
        catalystType.keyType,
        catalystType.valueType)
    }

    override def getConverter(fieldIndex: Int): Converter = keyValueConverter

    override def end(): Unit =
      updater.set(ArrayBasedMapData(currentKeys.toArray, currentValues.toArray))

    // NOTE: We can't reuse the mutable Map here and must instantiate a new `Map` for the next
    // value.  `Row.copy()` only copies row cells, it doesn't do deep copy to objects stored in row
    // cells.
    override def start(): Unit = {
      currentKeys = ArrayBuffer.empty[Any]
      currentValues = ArrayBuffer.empty[Any]
    }

    /** Parquet converter for key-value pairs within the map. */
    private final class KeyValueConverter(
        parquetKeyType: Type,
        parquetValueType: Type,
        catalystKeyType: DataType,
        catalystValueType: DataType)
      extends GroupConverter {

      private var currentKey: Any = _

      private var currentValue: Any = _

      private val converters = Array(
        // Converter for keys
        newConverter(parquetKeyType, catalystKeyType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentKey = value
        }),

        // Converter for values
        newConverter(parquetValueType, catalystValueType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentValue = value
        }))

      override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

      override def end(): Unit = {
        currentKeys += currentKey
        currentValues += currentValue
      }

      override def start(): Unit = {
        currentKey = null
        currentValue = null
      }
    }
  }

  private trait RepeatedConverter {
    private var currentArray: ArrayBuffer[Any] = _

    protected def newArrayUpdater(updater: ParentContainerUpdater) = new ParentContainerUpdater {
      override def start(): Unit = currentArray = ArrayBuffer.empty[Any]
      override def end(): Unit = updater.set(new GenericArrayData(currentArray.toArray))
      override def set(value: Any): Unit = currentArray += value
    }
  }

  /**
   * A primitive converter for converting unannotated repeated primitive values to required arrays
   * of required primitives values.
   */
  private final class RepeatedPrimitiveConverter(
      parquetType: Type,
      catalystType: DataType,
      parentUpdater: ParentContainerUpdater)
    extends PrimitiveConverter with RepeatedConverter with HasParentContainerUpdater {

    val updater: ParentContainerUpdater = newArrayUpdater(parentUpdater)

    private val elementConverter: PrimitiveConverter =
      newConverter(parquetType, catalystType, updater).asPrimitiveConverter()

    override def addBoolean(value: Boolean): Unit = elementConverter.addBoolean(value)
    override def addInt(value: Int): Unit = elementConverter.addInt(value)
    override def addLong(value: Long): Unit = elementConverter.addLong(value)
    override def addFloat(value: Float): Unit = elementConverter.addFloat(value)
    override def addDouble(value: Double): Unit = elementConverter.addDouble(value)
    override def addBinary(value: Binary): Unit = elementConverter.addBinary(value)

    override def setDictionary(dict: Dictionary): Unit = elementConverter.setDictionary(dict)
    override def hasDictionarySupport: Boolean = elementConverter.hasDictionarySupport
    override def addValueFromDictionary(id: Int): Unit = elementConverter.addValueFromDictionary(id)
  }

  /**
   * A group converter for converting unannotated repeated group values to required arrays of
   * required struct values.
   */
  private final class RepeatedGroupConverter(
      parquetType: Type,
      catalystType: DataType,
      parentUpdater: ParentContainerUpdater)
    extends GroupConverter with HasParentContainerUpdater with RepeatedConverter {

    val updater: ParentContainerUpdater = newArrayUpdater(parentUpdater)

    private val elementConverter: GroupConverter =
      newConverter(parquetType, catalystType, updater).asGroupConverter()

    override def getConverter(field: Int): Converter = elementConverter.getConverter(field)
    override def end(): Unit = elementConverter.end()
    override def start(): Unit = elementConverter.start()
  }
}

private[parquet] object CatalystRowConverter {
  def binaryToUnscaledLong(binary: Binary): Long = {
    // The underlying `ByteBuffer` implementation is guaranteed to be `HeapByteBuffer`, so here
    // we are using `Binary.toByteBuffer.array()` to steal the underlying byte array without
    // copying it.
    val buffer = binary.toByteBuffer
    val bytes = buffer.array()
    val start = buffer.position()
    val end = buffer.limit()

    var unscaled = 0L
    var i = start

    while (i < end) {
      unscaled = (unscaled << 8) | (bytes(i) & 0xff)
      i += 1
    }

    val bits = 8 * (end - start)
    unscaled = (unscaled << (64 - bits)) >> (64 - bits)
    unscaled
  }
}
