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

package org.apache.spark.sql.catalyst.expressions

import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.serializer._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.plans.logical.{Project, LocalRelation}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.types._

/**
 * Invokes a static function, returning the result.  By default, any of the arguments being null
 * will result in returning null instead of calling the function.
 *
 * @param staticObject The target of the static call.  This can either be the object itself
 *                     (methods defined on scala objects), or the class object
 *                     (static methods defined in java).
 * @param dataType The expected return type of the function call
 * @param functionName The name of the method to call.
 * @param arguments An optional list of expressions to pass as arguments to the function.
 * @param propagateNull When true, and any of the arguments is null, null will be returned instead
 *                      of calling the function.
 */
case class StaticInvoke(
    staticObject: Class[_],
    dataType: DataType,
    functionName: String,
    arguments: Seq[Expression] = Nil,
    propagateNull: Boolean = true) extends Expression {

  val objectName = staticObject.getName.stripSuffix("$")

  override def nullable: Boolean = true
  override def children: Seq[Expression] = arguments

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val javaType = ctx.javaType(dataType)
    val argGen = arguments.map(_.gen(ctx))
    val argString = argGen.map(_.value).mkString(", ")

    if (propagateNull) {
      val objNullCheck = if (ctx.defaultValue(dataType) == "null") {
        s"${ev.isNull} = ${ev.value} == null;"
      } else {
        ""
      }

      val argsNonNull = s"!(${argGen.map(_.isNull).mkString(" || ")})"
      s"""
        ${argGen.map(_.code).mkString("\n")}

        boolean ${ev.isNull} = !$argsNonNull;
        $javaType ${ev.value} = ${ctx.defaultValue(dataType)};

        if ($argsNonNull) {
          ${ev.value} = $objectName.$functionName($argString);
          $objNullCheck
        }
       """
    } else {
      s"""
        ${argGen.map(_.code).mkString("\n")}

        $javaType ${ev.value} = $objectName.$functionName($argString);
        final boolean ${ev.isNull} = ${ev.value} == null;
      """
    }
  }
}

/**
 * Calls the specified function on an object, optionally passing arguments.  If the `targetObject`
 * expression evaluates to null then null will be returned.
 *
 * In some cases, due to erasure, the schema may expect a primitive type when in fact the method
 * is returning java.lang.Object.  In this case, we will generate code that attempts to unbox the
 * value automatically.
 *
 * @param targetObject An expression that will return the object to call the method on.
 * @param functionName The name of the method to call.
 * @param dataType The expected return type of the function.
 * @param arguments An optional list of expressions, whos evaluation will be passed to the function.
 */
case class Invoke(
    targetObject: Expression,
    functionName: String,
    dataType: DataType,
    arguments: Seq[Expression] = Nil) extends Expression {

  override def nullable: Boolean = true
  override def children: Seq[Expression] = arguments.+:(targetObject)

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  lazy val method = targetObject.dataType match {
    case ObjectType(cls) =>
      cls
        .getMethods
        .find(_.getName == functionName)
        .getOrElse(sys.error(s"Couldn't find $functionName on $cls"))
        .getReturnType
        .getName
    case _ => ""
  }

  lazy val unboxer = (dataType, method) match {
    case (IntegerType, "java.lang.Object") => (s: String) =>
      s"((java.lang.Integer)$s).intValue()"
    case (LongType, "java.lang.Object") => (s: String) =>
      s"((java.lang.Long)$s).longValue()"
    case (FloatType, "java.lang.Object") => (s: String) =>
      s"((java.lang.Float)$s).floatValue()"
    case (ShortType, "java.lang.Object") => (s: String) =>
      s"((java.lang.Short)$s).shortValue()"
    case (ByteType, "java.lang.Object") => (s: String) =>
      s"((java.lang.Byte)$s).byteValue()"
    case (DoubleType, "java.lang.Object") => (s: String) =>
      s"((java.lang.Double)$s).doubleValue()"
    case (BooleanType, "java.lang.Object") => (s: String) =>
      s"((java.lang.Boolean)$s).booleanValue()"
    case _ => identity[String] _
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val javaType = ctx.javaType(dataType)
    val obj = targetObject.gen(ctx)
    val argGen = arguments.map(_.gen(ctx))
    val argString = argGen.map(_.value).mkString(", ")

    // If the function can return null, we do an extra check to make sure our null bit is still set
    // correctly.
    val objNullCheck = if (ctx.defaultValue(dataType) == "null") {
      s"${ev.isNull} = ${ev.value} == null;"
    } else {
      ""
    }

    val value = unboxer(s"${obj.value}.$functionName($argString)")

    s"""
      ${obj.code}
      ${argGen.map(_.code).mkString("\n")}

      boolean ${ev.isNull} = ${obj.value} == null;
      $javaType ${ev.value} =
        ${ev.isNull} ?
        ${ctx.defaultValue(dataType)} : ($javaType) $value;
      $objNullCheck
    """
  }
}

object NewInstance {
  def apply(
      cls: Class[_],
      arguments: Seq[Expression],
      propagateNull: Boolean = false,
      dataType: DataType): NewInstance =
    new NewInstance(cls, arguments, propagateNull, dataType, None)
}

/**
 * Constructs a new instance of the given class, using the result of evaluating the specified
 * expressions as arguments.
 *
 * @param cls The class to construct.
 * @param arguments A list of expression to use as arguments to the constructor.
 * @param propagateNull When true, if any of the arguments is null, then null will be returned
 *                      instead of trying to construct the object.
 * @param dataType The type of object being constructed, as a Spark SQL datatype.  This allows you
 *                 to manually specify the type when the object in question is a valid internal
 *                 representation (i.e. ArrayData) instead of an object.
 * @param outerPointer If the object being constructed is an inner class the outerPointer must
 *                     for the containing class must be specified.
 */
case class NewInstance(
    cls: Class[_],
    arguments: Seq[Expression],
    propagateNull: Boolean,
    dataType: DataType,
    outerPointer: Option[Literal]) extends Expression {
  private val className = cls.getName

  override def nullable: Boolean = propagateNull

  override def children: Seq[Expression] = arguments

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val javaType = ctx.javaType(dataType)
    val argGen = arguments.map(_.gen(ctx))
    val argString = argGen.map(_.value).mkString(", ")

    val outer = outerPointer.map(_.gen(ctx))

    val setup =
      s"""
         ${argGen.map(_.code).mkString("\n")}
         ${outer.map(_.code.mkString("")).getOrElse("")}
       """.stripMargin

    val constructorCall = outer.map { gen =>
      s"""${gen.value}.new ${cls.getSimpleName}($argString)"""
    }.getOrElse {
      s"new $className($argString)"
    }

    if (propagateNull) {
      val argsNonNull = s"!(${argGen.map(_.isNull).mkString(" || ")})"

      s"""
        $setup

        boolean ${ev.isNull} = true;
        $javaType ${ev.value} = ${ctx.defaultValue(dataType)};
        if ($argsNonNull) {
          ${ev.value} = $constructorCall;
          ${ev.isNull} = false;
        }
       """
    } else {
      s"""
        $setup

        $javaType ${ev.value} = $constructorCall;
        final boolean ${ev.isNull} = ${ev.value} == null;
      """
    }
  }
}

/**
 * Given an expression that returns on object of type `Option[_]`, this expression unwraps the
 * option into the specified Spark SQL datatype.  In the case of `None`, the nullbit is set instead.
 *
 * @param dataType The expected unwrapped option type.
 * @param child An expression that returns an `Option`
 */
case class UnwrapOption(
    dataType: DataType,
    child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = ObjectType :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val javaType = ctx.javaType(dataType)
    val inputObject = child.gen(ctx)

    s"""
      ${inputObject.code}

      boolean ${ev.isNull} = ${inputObject.value} == null || ${inputObject.value}.isEmpty();
      $javaType ${ev.value} =
        ${ev.isNull} ? ${ctx.defaultValue(dataType)} : ($javaType)${inputObject.value}.get();
    """
  }
}

/**
 * Converts the result of evaluating `child` into an option, checking both the isNull bit and
 * (in the case of reference types) equality with null.
 * @param child The expression to evaluate and wrap.
 */
case class WrapOption(child: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = ObjectType(classOf[Option[_]])

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = ObjectType :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val inputObject = child.gen(ctx)

    s"""
      ${inputObject.code}

      boolean ${ev.isNull} = false;
      scala.Option ${ev.value} =
        ${inputObject.isNull} ?
        scala.Option$$.MODULE$$.apply(null) : new scala.Some(${inputObject.value});
    """
  }
}

/**
 * A place holder for the loop variable used in [[MapObjects]].  This should never be constructed
 * manually, but will instead be passed into the provided lambda function.
 */
case class LambdaVariable(value: String, isNull: String, dataType: DataType) extends Expression {

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    throw new UnsupportedOperationException("Only calling gen() is supported.")

  override def children: Seq[Expression] = Nil
  override def gen(ctx: CodeGenContext): GeneratedExpressionCode =
    GeneratedExpressionCode(code = "", value = value, isNull = isNull)

  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

}

/**
 * Applies the given expression to every element of a collection of items, returning the result
 * as an ArrayType.  This is similar to a typical map operation, but where the lambda function
 * is expressed using catalyst expressions.
 *
 * The following collection ObjectTypes are currently supported:
 *   Seq, Array, ArrayData, java.util.List
 *
 * @param function A function that returns an expression, given an attribute that can be used
 *                 to access the current value.  This is does as a lambda function so that
 *                 a unique attribute reference can be provided for each expression (thus allowing
 *                 us to nest multiple MapObject calls).
 * @param inputData An expression that when evaluted returns a collection object.
 * @param elementType The type of element in the collection, expressed as a DataType.
 */
case class MapObjects(
    function: AttributeReference => Expression,
    inputData: Expression,
    elementType: DataType) extends Expression {

  private lazy val loopAttribute = AttributeReference("loopVar", elementType)()
  private lazy val completeFunction = function(loopAttribute)

  private def itemAccessorMethod(dataType: DataType): String => String = dataType match {
    case NullType =>
      val nullTypeClassName = NullType.getClass.getName + ".MODULE$"
      (i: String) => s".get($i, $nullTypeClassName)"
    case IntegerType => (i: String) => s".getInt($i)"
    case LongType => (i: String) => s".getLong($i)"
    case FloatType => (i: String) => s".getFloat($i)"
    case DoubleType => (i: String) => s".getDouble($i)"
    case ByteType => (i: String) => s".getByte($i)"
    case ShortType => (i: String) => s".getShort($i)"
    case BooleanType => (i: String) => s".getBoolean($i)"
    case StringType => (i: String) => s".getUTF8String($i)"
    case s: StructType => (i: String) => s".getStruct($i, ${s.size})"
    case a: ArrayType => (i: String) => s".getArray($i)"
    case _: MapType => (i: String) => s".getMap($i)"
    case udt: UserDefinedType[_] => itemAccessorMethod(udt.sqlType)
  }

  private lazy val (lengthFunction, itemAccessor, primitiveElement) = inputData.dataType match {
    case ObjectType(cls) if classOf[Seq[_]].isAssignableFrom(cls) =>
      (".size()", (i: String) => s".apply($i)", false)
    case ObjectType(cls) if cls.isArray =>
      (".length", (i: String) => s"[$i]", false)
    case ObjectType(cls) if classOf[java.util.List[_]].isAssignableFrom(cls) =>
      (".size()", (i: String) => s".get($i)", false)
    case ArrayType(t, _) =>
      val (sqlType, primitiveElement) = t match {
        case m: MapType => (m, false)
        case s: StructType => (s, false)
        case s: StringType => (s, false)
        case udt: UserDefinedType[_] => (udt.sqlType, false)
        case o => (o, true)
      }
      (".numElements()", itemAccessorMethod(sqlType), primitiveElement)
  }

  override def nullable: Boolean = true

  override def children: Seq[Expression] = completeFunction :: inputData :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def dataType: DataType = ArrayType(completeFunction.dataType)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val javaType = ctx.javaType(dataType)
    val elementJavaType = ctx.javaType(elementType)
    val genInputData = inputData.gen(ctx)

    // Variables to hold the element that is currently being processed.
    val loopValue = ctx.freshName("loopValue")
    val loopIsNull = ctx.freshName("loopIsNull")

    val loopVariable = LambdaVariable(loopValue, loopIsNull, elementType)
    val substitutedFunction = completeFunction transform {
      case a: AttributeReference if a == loopAttribute => loopVariable
    }
    // A hack to run this through the analyzer (to bind extractions).
    val boundFunction =
      SimpleAnalyzer.execute(Project(Alias(substitutedFunction, "")() :: Nil, LocalRelation(Nil)))
        .expressions.head.children.head

    val genFunction = boundFunction.gen(ctx)
    val dataLength = ctx.freshName("dataLength")
    val convertedArray = ctx.freshName("convertedArray")
    val loopIndex = ctx.freshName("loopIndex")

    val convertedType = ctx.boxedType(boundFunction.dataType)

    // Because of the way Java defines nested arrays, we have to handle the syntax specially.
    // Specifically, we have to insert the [$dataLength] in between the type and any extra nested
    // array declarations (i.e. new String[1][]).
    val arrayConstructor = if (convertedType contains "[]") {
      val rawType = convertedType.takeWhile(_ != '[')
      val arrayPart = convertedType.reverse.takeWhile(c => c == '[' || c == ']').reverse
      s"new $rawType[$dataLength]$arrayPart"
    } else {
      s"new $convertedType[$dataLength]"
    }

    val loopNullCheck = if (primitiveElement) {
      s"boolean $loopIsNull = ${genInputData.value}.isNullAt($loopIndex);"
    } else {
      s"boolean $loopIsNull = ${genInputData.isNull} || $loopValue == null;"
    }

    s"""
      ${genInputData.code}

      boolean ${ev.isNull} = ${genInputData.value} == null;
      $javaType ${ev.value} = ${ctx.defaultValue(dataType)};

      if (!${ev.isNull}) {
        $convertedType[] $convertedArray = null;
        int $dataLength = ${genInputData.value}$lengthFunction;
        $convertedArray = $arrayConstructor;

        int $loopIndex = 0;
        while ($loopIndex < $dataLength) {
          $elementJavaType $loopValue =
            ($elementJavaType)${genInputData.value}${itemAccessor(loopIndex)};
          $loopNullCheck

          if ($loopIsNull) {
            $convertedArray[$loopIndex] = null;
          } else {
            ${genFunction.code}
            $convertedArray[$loopIndex] = ${genFunction.value};
          }

          $loopIndex += 1;
        }

        ${ev.isNull} = false;
        ${ev.value} = new ${classOf[GenericArrayData].getName}($convertedArray);
      }
    """
  }
}

/**
 * Constructs a new external row, using the result of evaluating the specified expressions
 * as content.
 *
 * @param children A list of expression to use as content of the external row.
 */
case class CreateExternalRow(children: Seq[Expression]) extends Expression {
  override def dataType: DataType = ObjectType(classOf[Row])

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val rowClass = classOf[GenericRow].getName
    val values = ctx.freshName("values")
    s"""
      boolean ${ev.isNull} = false;
      final Object[] $values = new Object[${children.size}];
    """ +
      children.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
          if (${eval.isNull}) {
            $values[$i] = null;
          } else {
            $values[$i] = ${eval.value};
          }
         """
      }.mkString("\n") +
      s"final ${classOf[Row].getName} ${ev.value} = new $rowClass($values);"
  }
}

/**
 * Serializes an input object using a generic serializer (Kryo or Java).
 * @param kryo if true, use Kryo. Otherwise, use Java.
 */
case class EncodeUsingSerializer(child: Expression, kryo: Boolean) extends UnaryExpression {

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    // Code to initialize the serializer.
    val serializer = ctx.freshName("serializer")
    val (serializerClass, serializerInstanceClass) = {
      if (kryo) {
        (classOf[KryoSerializer].getName, classOf[KryoSerializerInstance].getName)
      } else {
        (classOf[JavaSerializer].getName, classOf[JavaSerializerInstance].getName)
      }
    }
    val sparkConf = s"new ${classOf[SparkConf].getName}()"
    ctx.addMutableState(
      serializerInstanceClass,
      serializer,
      s"$serializer = ($serializerInstanceClass) new $serializerClass($sparkConf).newInstance();")

    // Code to serialize.
    val input = child.gen(ctx)
    s"""
      ${input.code}
      final boolean ${ev.isNull} = ${input.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = $serializer.serialize(${input.value}, null).array();
      }
     """
  }

  override def dataType: DataType = BinaryType
}

/**
 * Serializes an input object using a generic serializer (Kryo or Java).  Note that the ClassTag
 * is not an implicit parameter because TreeNode cannot copy implicit parameters.
 * @param kryo if true, use Kryo. Otherwise, use Java.
 */
case class DecodeUsingSerializer[T](child: Expression, tag: ClassTag[T], kryo: Boolean)
  extends UnaryExpression {

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    // Code to initialize the serializer.
    val serializer = ctx.freshName("serializer")
    val (serializerClass, serializerInstanceClass) = {
      if (kryo) {
        (classOf[KryoSerializer].getName, classOf[KryoSerializerInstance].getName)
      } else {
        (classOf[JavaSerializer].getName, classOf[JavaSerializerInstance].getName)
      }
    }
    val sparkConf = s"new ${classOf[SparkConf].getName}()"
    ctx.addMutableState(
      serializerInstanceClass,
      serializer,
      s"$serializer = ($serializerInstanceClass) new $serializerClass($sparkConf).newInstance();")

    // Code to serialize.
    val input = child.gen(ctx)
    s"""
      ${input.code}
      final boolean ${ev.isNull} = ${input.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = (${ctx.javaType(dataType)})
          $serializer.deserialize(java.nio.ByteBuffer.wrap(${input.value}), null);
      }
     """
  }

  override def dataType: DataType = ObjectType(tag.runtimeClass)
}

/**
 * Initialize a Java Bean instance by setting its field values via setters.
 */
case class InitializeJavaBean(beanInstance: Expression, setters: Map[String, Expression])
  extends Expression {

  override def nullable: Boolean = beanInstance.nullable
  override def children: Seq[Expression] = beanInstance +: setters.values.toSeq
  override def dataType: DataType = beanInstance.dataType

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val instanceGen = beanInstance.gen(ctx)

    val initialize = setters.map {
      case (setterMethod, fieldValue) =>
        val fieldGen = fieldValue.gen(ctx)
        s"""
           ${fieldGen.code}
           ${instanceGen.value}.$setterMethod(${fieldGen.value});
         """
    }

    ev.isNull = instanceGen.isNull
    ev.value = instanceGen.value

    s"""
      ${instanceGen.code}
      if (!${instanceGen.isNull}) {
        ${initialize.mkString("\n")}
      }
     """
  }
}
