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

import java.text.DecimalFormat
import java.util.{HashMap, Locale, Map => JMap}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines expressions for string operations.
////////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * An expression that concatenates multiple input strings into a single string.
 * If any input is null, concat returns null.
 */
case class Concat(children: Seq[Expression]) extends Expression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(StringType)
  override def dataType: DataType = StringType

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    val inputs = children.map(_.eval(input).asInstanceOf[UTF8String])
    UTF8String.concat(inputs : _*)
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val evals = children.map(_.gen(ctx))
    val inputs = evals.map { eval =>
      s"${eval.isNull} ? null : ${eval.value}"
    }.mkString(", ")
    evals.map(_.code).mkString("\n") + s"""
      boolean ${ev.isNull} = false;
      UTF8String ${ev.value} = UTF8String.concat($inputs);
      if (${ev.value} == null) {
        ${ev.isNull} = true;
      }
    """
  }
}


/**
 * An expression that concatenates multiple input strings or array of strings into a single string,
 * using a given separator (the first child).
 *
 * Returns null if the separator is null. Otherwise, concat_ws skips all null values.
 */
case class ConcatWs(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  require(children.nonEmpty, s"$prettyName requires at least one argument.")

  override def prettyName: String = "concat_ws"

  /** The 1st child (separator) is str, and rest are either str or array of str. */
  override def inputTypes: Seq[AbstractDataType] = {
    val arrayOrStr = TypeCollection(ArrayType(StringType), StringType)
    StringType +: Seq.fill(children.size - 1)(arrayOrStr)
  }

  override def dataType: DataType = StringType

  override def nullable: Boolean = children.head.nullable
  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    val flatInputs = children.flatMap { child =>
      child.eval(input) match {
        case s: UTF8String => Iterator(s)
        case arr: ArrayData => arr.toArray[UTF8String](StringType)
        case null => Iterator(null.asInstanceOf[UTF8String])
      }
    }
    UTF8String.concatWs(flatInputs.head, flatInputs.tail : _*)
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (children.forall(_.dataType == StringType)) {
      // All children are strings. In that case we can construct a fixed size array.
      val evals = children.map(_.gen(ctx))

      val inputs = evals.map { eval =>
        s"${eval.isNull} ? (UTF8String) null : ${eval.value}"
      }.mkString(", ")

      evals.map(_.code).mkString("\n") + s"""
        UTF8String ${ev.value} = UTF8String.concatWs($inputs);
        boolean ${ev.isNull} = ${ev.value} == null;
      """
    } else {
      val array = ctx.freshName("array")
      val varargNum = ctx.freshName("varargNum")
      val idxInVararg = ctx.freshName("idxInVararg")

      val evals = children.map(_.gen(ctx))
      val (varargCount, varargBuild) = children.tail.zip(evals.tail).map { case (child, eval) =>
        child.dataType match {
          case StringType =>
            ("", // we count all the StringType arguments num at once below.
              s"$array[$idxInVararg ++] = ${eval.isNull} ? (UTF8String) null : ${eval.value};")
          case _: ArrayType =>
            val size = ctx.freshName("n")
            (s"""
              if (!${eval.isNull}) {
                $varargNum += ${eval.value}.numElements();
              }
            """,
            s"""
            if (!${eval.isNull}) {
              final int $size = ${eval.value}.numElements();
              for (int j = 0; j < $size; j ++) {
                $array[$idxInVararg ++] = ${ctx.getValue(eval.value, StringType, "j")};
              }
            }
            """)
        }
      }.unzip

      evals.map(_.code).mkString("\n") +
      s"""
        int $varargNum = ${children.count(_.dataType == StringType) - 1};
        int $idxInVararg = 0;
        ${varargCount.mkString("\n")}
        UTF8String[] $array = new UTF8String[$varargNum];
        ${varargBuild.mkString("\n")}
        UTF8String ${ev.value} = UTF8String.concatWs(${evals.head.value}, $array);
        boolean ${ev.isNull} = ${ev.value} == null;
      """
    }
  }
}

trait String2StringExpression extends ImplicitCastInputTypes {
  self: UnaryExpression =>

  def convert(v: UTF8String): UTF8String

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  protected override def nullSafeEval(input: Any): Any =
    convert(input.asInstanceOf[UTF8String])
}

/**
 * A function that converts the characters of a string to uppercase.
 */
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns str with all characters changed to uppercase",
  extended = "> SELECT _FUNC_('SparkSql');\n 'SPARKSQL'")
case class Upper(child: Expression)
  extends UnaryExpression with String2StringExpression {

  override def convert(v: UTF8String): UTF8String = v.toUpperCase

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).toUpperCase()")
  }
}

/**
 * A function that converts the characters of a string to lowercase.
 */
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns str with all characters changed to lowercase",
  extended = "> SELECT _FUNC_('SparkSql');\n'sparksql'")
case class Lower(child: Expression) extends UnaryExpression with String2StringExpression {

  override def convert(v: UTF8String): UTF8String = v.toLowerCase

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).toLowerCase()")
  }
}

/** A base trait for functions that compare two strings, returning a boolean. */
trait StringPredicate extends Predicate with ImplicitCastInputTypes {
  self: BinaryExpression =>

  def compare(l: UTF8String, r: UTF8String): Boolean

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    compare(input1.asInstanceOf[UTF8String], input2.asInstanceOf[UTF8String])

  override def toString: String = s"$nodeName($left, $right)"
}

/**
 * A function that returns true if the string `left` contains the string `right`.
 */
case class Contains(left: Expression, right: Expression)
    extends BinaryExpression with StringPredicate {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.contains(r)
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).contains($c2)")
  }
}

/**
 * A function that returns true if the string `left` starts with the string `right`.
 */
case class StartsWith(left: Expression, right: Expression)
    extends BinaryExpression with StringPredicate {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.startsWith(r)
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).startsWith($c2)")
  }
}

/**
 * A function that returns true if the string `left` ends with the string `right`.
 */
case class EndsWith(left: Expression, right: Expression)
    extends BinaryExpression with StringPredicate {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.endsWith(r)
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).endsWith($c2)")
  }
}

object StringTranslate {

  def buildDict(matchingString: UTF8String, replaceString: UTF8String)
    : JMap[Character, Character] = {
    val matching = matchingString.toString()
    val replace = replaceString.toString()
    val dict = new HashMap[Character, Character]()
    var i = 0
    while (i < matching.length()) {
      val rep = if (i < replace.length()) replace.charAt(i) else '\u0000'
      if (null == dict.get(matching.charAt(i))) {
        dict.put(matching.charAt(i), rep)
      }
      i += 1
    }
    dict
  }
}

/**
 * A function translate any character in the `srcExpr` by a character in `replaceExpr`.
 * The characters in `replaceExpr` is corresponding to the characters in `matchingExpr`.
 * The translate will happen when any character in the string matching with the character
 * in the `matchingExpr`.
 */
case class StringTranslate(srcExpr: Expression, matchingExpr: Expression, replaceExpr: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  @transient private var lastMatching: UTF8String = _
  @transient private var lastReplace: UTF8String = _
  @transient private var dict: JMap[Character, Character] = _

  override def nullSafeEval(srcEval: Any, matchingEval: Any, replaceEval: Any): Any = {
    if (matchingEval != lastMatching || replaceEval != lastReplace) {
      lastMatching = matchingEval.asInstanceOf[UTF8String].clone()
      lastReplace = replaceEval.asInstanceOf[UTF8String].clone()
      dict = StringTranslate.buildDict(lastMatching, lastReplace)
    }
    srcEval.asInstanceOf[UTF8String].translate(dict)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val termLastMatching = ctx.freshName("lastMatching")
    val termLastReplace = ctx.freshName("lastReplace")
    val termDict = ctx.freshName("dict")
    val classNameDict = classOf[JMap[Character, Character]].getCanonicalName

    ctx.addMutableState("UTF8String", termLastMatching, s"${termLastMatching} = null;")
    ctx.addMutableState("UTF8String", termLastReplace, s"${termLastReplace} = null;")
    ctx.addMutableState(classNameDict, termDict, s"${termDict} = null;")

    nullSafeCodeGen(ctx, ev, (src, matching, replace) => {
      val check = if (matchingExpr.foldable && replaceExpr.foldable) {
        s"${termDict} == null"
      } else {
        s"!${matching}.equals(${termLastMatching}) || !${replace}.equals(${termLastReplace})"
      }
      s"""if ($check) {
        // Not all of them is literal or matching or replace value changed
        ${termLastMatching} = ${matching}.clone();
        ${termLastReplace} = ${replace}.clone();
        ${termDict} = org.apache.spark.sql.catalyst.expressions.StringTranslate
          .buildDict(${termLastMatching}, ${termLastReplace});
      }
      ${ev.value} = ${src}.translate(${termDict});
      """
    })
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)
  override def children: Seq[Expression] = srcExpr :: matchingExpr :: replaceExpr :: Nil
  override def prettyName: String = "translate"
}

/**
 * A function that returns the index (1-based) of the given string (left) in the comma-
 * delimited list (right). Returns 0, if the string wasn't found or if the given
 * string (left) contains a comma.
 */
case class FindInSet(left: Expression, right: Expression) extends BinaryExpression
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override protected def nullSafeEval(word: Any, set: Any): Any =
    set.asInstanceOf[UTF8String].findInSet(word.asInstanceOf[UTF8String])

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (word, set) =>
      s"${ev.value} = $set.findInSet($word);"
    )
  }

  override def dataType: DataType = IntegerType
}

/**
 * A function that trim the spaces from both ends for the specified string.
 */
case class StringTrim(child: Expression)
  extends UnaryExpression with String2StringExpression {

  def convert(v: UTF8String): UTF8String = v.trim()

  override def prettyName: String = "trim"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).trim()")
  }
}

/**
 * A function that trim the spaces from left end for given string.
 */
case class StringTrimLeft(child: Expression)
  extends UnaryExpression with String2StringExpression {

  def convert(v: UTF8String): UTF8String = v.trimLeft()

  override def prettyName: String = "ltrim"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).trimLeft()")
  }
}

/**
 * A function that trim the spaces from right end for given string.
 */
case class StringTrimRight(child: Expression)
  extends UnaryExpression with String2StringExpression {

  def convert(v: UTF8String): UTF8String = v.trimRight()

  override def prettyName: String = "rtrim"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).trimRight()")
  }
}

/**
 * A function that returns the position of the first occurrence of substr in the given string.
 * Returns null if either of the arguments are null and
 * returns 0 if substr could not be found in str.
 *
 * NOTE: that this is not zero based, but 1-based index. The first character in str has index 1.
 */
case class StringInstr(str: Expression, substr: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = str
  override def right: Expression = substr
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  override def nullSafeEval(string: Any, sub: Any): Any = {
    string.asInstanceOf[UTF8String].indexOf(sub.asInstanceOf[UTF8String], 0) + 1
  }

  override def prettyName: String = "instr"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (l, r) =>
      s"($l).indexOf($r, 0) + 1")
  }
}

/**
 * Returns the substring from string str before count occurrences of the delimiter delim.
 * If count is positive, everything the left of the final delimiter (counting from left) is
 * returned. If count is negative, every to the right of the final delimiter (counting from the
 * right) is returned. substring_index performs a case-sensitive match when searching for delim.
 */
case class SubstringIndex(strExpr: Expression, delimExpr: Expression, countExpr: Expression)
 extends TernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = Seq(strExpr, delimExpr, countExpr)
  override def prettyName: String = "substring_index"

  override def nullSafeEval(str: Any, delim: Any, count: Any): Any = {
    str.asInstanceOf[UTF8String].subStringIndex(
      delim.asInstanceOf[UTF8String],
      count.asInstanceOf[Int])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (str, delim, count) => s"$str.subStringIndex($delim, $count)")
  }
}

/**
 * A function that returns the position of the first occurrence of substr
 * in given string after position pos.
 */
case class StringLocate(substr: Expression, str: Expression, start: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  def this(substr: Expression, str: Expression) = {
    this(substr, str, Literal(0))
  }

  override def children: Seq[Expression] = substr :: str :: start :: Nil
  override def nullable: Boolean = substr.nullable || str.nullable
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)

  override def eval(input: InternalRow): Any = {
    val s = start.eval(input)
    if (s == null) {
      // if the start position is null, we need to return 0, (conform to Hive)
      0
    } else {
      val r = substr.eval(input)
      if (r == null) {
        null
      } else {
        val l = str.eval(input)
        if (l == null) {
          null
        } else {
          l.asInstanceOf[UTF8String].indexOf(
            r.asInstanceOf[UTF8String],
            s.asInstanceOf[Int]) + 1
        }
      }
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val substrGen = substr.gen(ctx)
    val strGen = str.gen(ctx)
    val startGen = start.gen(ctx)
    s"""
      int ${ev.value} = 0;
      boolean ${ev.isNull} = false;
      ${startGen.code}
      if (!${startGen.isNull}) {
        ${substrGen.code}
        if (!${substrGen.isNull}) {
          ${strGen.code}
          if (!${strGen.isNull}) {
            ${ev.value} = ${strGen.value}.indexOf(${substrGen.value},
              ${startGen.value}) + 1;
          } else {
            ${ev.isNull} = true;
          }
        } else {
          ${ev.isNull} = true;
        }
      }
     """
  }

  override def prettyName: String = "locate"
}

/**
 * Returns str, left-padded with pad to a length of len.
 */
case class StringLPad(str: Expression, len: Expression, pad: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  override def children: Seq[Expression] = str :: len :: pad :: Nil
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def nullSafeEval(str: Any, len: Any, pad: Any): Any = {
    str.asInstanceOf[UTF8String].lpad(len.asInstanceOf[Int], pad.asInstanceOf[UTF8String])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (str, len, pad) => s"$str.lpad($len, $pad)")
  }

  override def prettyName: String = "lpad"
}

/**
 * Returns str, right-padded with pad to a length of len.
 */
case class StringRPad(str: Expression, len: Expression, pad: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  override def children: Seq[Expression] = str :: len :: pad :: Nil
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def nullSafeEval(str: Any, len: Any, pad: Any): Any = {
    str.asInstanceOf[UTF8String].rpad(len.asInstanceOf[Int], pad.asInstanceOf[UTF8String])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (str, len, pad) => s"$str.rpad($len, $pad)")
  }

  override def prettyName: String = "rpad"
}

/**
 * Returns the input formatted according do printf-style format strings
 */
case class FormatString(children: Expression*) extends Expression with ImplicitCastInputTypes {

  require(children.nonEmpty, "format_string() should take at least 1 argument")

  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = children(0).nullable
  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] =
    StringType :: List.fill(children.size - 1)(AnyDataType)

  override def eval(input: InternalRow): Any = {
    val pattern = children(0).eval(input)
    if (pattern == null) {
      null
    } else {
      val sb = new StringBuffer()
      val formatter = new java.util.Formatter(sb, Locale.US)

      val arglist = children.tail.map(_.eval(input).asInstanceOf[AnyRef])
      formatter.format(pattern.asInstanceOf[UTF8String].toString, arglist: _*)

      UTF8String.fromString(sb.toString)
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val pattern = children.head.gen(ctx)

    val argListGen = children.tail.map(x => (x.dataType, x.gen(ctx)))
    val argListCode = argListGen.map(_._2.code + "\n")

    val argListString = argListGen.foldLeft("")((s, v) => {
      val nullSafeString =
        if (ctx.boxedType(v._1) != ctx.javaType(v._1)) {
          // Java primitives get boxed in order to allow null values.
          s"(${v._2.isNull}) ? (${ctx.boxedType(v._1)}) null : " +
            s"new ${ctx.boxedType(v._1)}(${v._2.value})"
        } else {
          s"(${v._2.isNull}) ? null : ${v._2.value}"
        }
      s + "," + nullSafeString
    })

    val form = ctx.freshName("formatter")
    val formatter = classOf[java.util.Formatter].getName
    val sb = ctx.freshName("sb")
    val stringBuffer = classOf[StringBuffer].getName
    s"""
      ${pattern.code}
      boolean ${ev.isNull} = ${pattern.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${argListCode.mkString}
        $stringBuffer $sb = new $stringBuffer();
        $formatter $form = new $formatter($sb, ${classOf[Locale].getName}.US);
        $form.format(${pattern.value}.toString() $argListString);
        ${ev.value} = UTF8String.fromString($sb.toString());
      }
     """
  }

  override def prettyName: String = "format_string"
}

/**
 * Returns string, with the first letter of each word in uppercase.
 * Words are delimited by whitespace.
 */
case class InitCap(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[DataType] = Seq(StringType)
  override def dataType: DataType = StringType

  override def nullSafeEval(string: Any): Any = {
    string.asInstanceOf[UTF8String].toTitleCase
  }
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, str => s"$str.toTitleCase()")
  }
}

/**
 * Returns the string which repeat the given string value n times.
 */
case class StringRepeat(str: Expression, times: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = str
  override def right: Expression = times
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType)

  override def nullSafeEval(string: Any, n: Any): Any = {
    string.asInstanceOf[UTF8String].repeat(n.asInstanceOf[Integer])
  }

  override def prettyName: String = "repeat"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (l, r) => s"($l).repeat($r)")
  }
}

/**
 * Returns the reversed given string.
 */
case class StringReverse(child: Expression) extends UnaryExpression with String2StringExpression {
  override def convert(v: UTF8String): UTF8String = v.reverse()

  override def prettyName: String = "reverse"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).reverse()")
  }
}

/**
 * Returns a n spaces string.
 */
case class StringSpace(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(IntegerType)

  override def nullSafeEval(s: Any): Any = {
    val length = s.asInstanceOf[Int]
    UTF8String.blankString(if (length < 0) 0 else length)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (length) =>
      s"""${ev.value} = UTF8String.blankString(($length < 0) ? 0 : $length);""")
  }

  override def prettyName: String = "space"
}

/**
 * A function that takes a substring of its first argument starting at a given position.
 * Defined for String and Binary types.
 */
case class Substring(str: Expression, pos: Expression, len: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  def this(str: Expression, pos: Expression) = {
    this(str, pos, Literal(Integer.MAX_VALUE))
  }

  override def dataType: DataType = str.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, BinaryType), IntegerType, IntegerType)

  override def children: Seq[Expression] = str :: pos :: len :: Nil

  override def nullSafeEval(string: Any, pos: Any, len: Any): Any = {
    str.dataType match {
      case StringType => string.asInstanceOf[UTF8String]
        .substringSQL(pos.asInstanceOf[Int], len.asInstanceOf[Int])
      case BinaryType => ByteArray.subStringSQL(string.asInstanceOf[Array[Byte]],
        pos.asInstanceOf[Int], len.asInstanceOf[Int])
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {

    defineCodeGen(ctx, ev, (string, pos, len) => {
      str.dataType match {
        case StringType => s"$string.substringSQL($pos, $len)"
        case BinaryType => s"${classOf[ByteArray].getName}.subStringSQL($string, $pos, $len)"
      }
    })
  }
}

/**
 * A function that return the length of the given string or binary expression.
 */
case class Length(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))

  protected override def nullSafeEval(value: Any): Any = child.dataType match {
    case StringType => value.asInstanceOf[UTF8String].numChars
    case BinaryType => value.asInstanceOf[Array[Byte]].length
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    child.dataType match {
      case StringType => defineCodeGen(ctx, ev, c => s"($c).numChars()")
      case BinaryType => defineCodeGen(ctx, ev, c => s"($c).length")
    }
  }
}

/**
 * A function that return the Levenshtein distance between the two given strings.
 */
case class Levenshtein(left: Expression, right: Expression) extends BinaryExpression
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def dataType: DataType = IntegerType
  protected override def nullSafeEval(leftValue: Any, rightValue: Any): Any =
    leftValue.asInstanceOf[UTF8String].levenshteinDistance(rightValue.asInstanceOf[UTF8String])

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (left, right) =>
      s"${ev.value} = $left.levenshteinDistance($right);")
  }
}

/**
 * A function that return soundex code of the given string expression.
 */
case class SoundEx(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def nullSafeEval(input: Any): Any = input.asInstanceOf[UTF8String].soundex()

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c.soundex()")
  }
}

/**
 * Returns the numeric value of the first character of str.
 */
case class Ascii(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  protected override def nullSafeEval(string: Any): Any = {
    val bytes = string.asInstanceOf[UTF8String].getBytes
    if (bytes.length > 0) {
      bytes(0).asInstanceOf[Int]
    } else {
      0
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (child) => {
      val bytes = ctx.freshName("bytes")
      s"""
        byte[] $bytes = $child.getBytes();
        if ($bytes.length > 0) {
          ${ev.value} = (int) $bytes[0];
        } else {
          ${ev.value} = 0;
        }
       """})
  }
}

/**
 * Converts the argument from binary to a base 64 string.
 */
case class Base64(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(bytes: Any): Any = {
    UTF8String.fromBytes(
      org.apache.commons.codec.binary.Base64.encodeBase64(
        bytes.asInstanceOf[Array[Byte]]))
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (child) => {
      s"""${ev.value} = UTF8String.fromBytes(
            org.apache.commons.codec.binary.Base64.encodeBase64($child));
       """})
  }

}

/**
 * Converts the argument from a base 64 string to BINARY.
 */
case class UnBase64(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  protected override def nullSafeEval(string: Any): Any =
    org.apache.commons.codec.binary.Base64.decodeBase64(string.asInstanceOf[UTF8String].toString)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (child) => {
      s"""
         ${ev.value} = org.apache.commons.codec.binary.Base64.decodeBase64($child.toString());
       """})
  }
}

/**
 * Decodes the first argument into a String using the provided character set
 * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
 * If either argument is null, the result will also be null.
 */
case class Decode(bin: Expression, charset: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = bin
  override def right: Expression = charset
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(BinaryType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val fromCharset = input2.asInstanceOf[UTF8String].toString
    UTF8String.fromString(new String(input1.asInstanceOf[Array[Byte]], fromCharset))
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (bytes, charset) =>
      s"""
        try {
          ${ev.value} = UTF8String.fromString(new String($bytes, $charset.toString()));
        } catch (java.io.UnsupportedEncodingException e) {
          org.apache.spark.unsafe.Platform.throwException(e);
        }
      """)
  }
}

/**
 * Encodes the first argument into a BINARY using the provided character set
 * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
 * If either argument is null, the result will also be null.
*/
case class Encode(value: Expression, charset: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = value
  override def right: Expression = charset
  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val toCharset = input2.asInstanceOf[UTF8String].toString
    input1.asInstanceOf[UTF8String].toString.getBytes(toCharset)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (string, charset) =>
      s"""
        try {
          ${ev.value} = $string.toString().getBytes($charset.toString());
        } catch (java.io.UnsupportedEncodingException e) {
          org.apache.spark.unsafe.Platform.throwException(e);
        }""")
  }
}

/**
 * Formats the number X to a format like '#,###,###.##', rounded to D decimal places,
 * and returns the result as a string. If D is 0, the result has no decimal point or
 * fractional part.
 */
case class FormatNumber(x: Expression, d: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def left: Expression = x
  override def right: Expression = d
  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType)

  // Associated with the pattern, for the last d value, and we will update the
  // pattern (DecimalFormat) once the new coming d value differ with the last one.
  @transient
  private var lastDValue: Int = -100

  // A cached DecimalFormat, for performance concern, we will change it
  // only if the d value changed.
  @transient
  private val pattern: StringBuffer = new StringBuffer()

  @transient
  private val numberFormat: DecimalFormat = new DecimalFormat("")

  override protected def nullSafeEval(xObject: Any, dObject: Any): Any = {
    val dValue = dObject.asInstanceOf[Int]
    if (dValue < 0) {
      return null
    }

    if (dValue != lastDValue) {
      // construct a new DecimalFormat only if a new dValue
      pattern.delete(0, pattern.length)
      pattern.append("#,###,###,###,###,###,##0")

      // decimal place
      if (dValue > 0) {
        pattern.append(".")

        var i = 0
        while (i < dValue) {
          i += 1
          pattern.append("0")
        }
      }
      val dFormat = new DecimalFormat(pattern.toString)
      lastDValue = dValue

      numberFormat.applyPattern(dFormat.toPattern)
    }

    x.dataType match {
      case ByteType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Byte]))
      case ShortType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Short]))
      case FloatType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Float]))
      case IntegerType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Int]))
      case LongType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Long]))
      case DoubleType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Double]))
      case _: DecimalType =>
        UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Decimal].toJavaBigDecimal))
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (num, d) => {

      def typeHelper(p: String): String = {
        x.dataType match {
          case _ : DecimalType => s"""$p.toJavaBigDecimal()"""
          case _ => s"$p"
        }
      }

      val sb = classOf[StringBuffer].getName
      val df = classOf[DecimalFormat].getName
      val lastDValue = ctx.freshName("lastDValue")
      val pattern = ctx.freshName("pattern")
      val numberFormat = ctx.freshName("numberFormat")
      val i = ctx.freshName("i")
      val dFormat = ctx.freshName("dFormat")
      ctx.addMutableState("int", lastDValue, s"$lastDValue = -100;")
      ctx.addMutableState(sb, pattern, s"$pattern = new $sb();")
      ctx.addMutableState(df, numberFormat, s"""$numberFormat = new $df("");""")

      s"""
        if ($d >= 0) {
          $pattern.delete(0, $pattern.length());
          if ($d != $lastDValue) {
            $pattern.append("#,###,###,###,###,###,##0");

            if ($d > 0) {
              $pattern.append(".");
              for (int $i = 0; $i < $d; $i++) {
                $pattern.append("0");
              }
            }
            $df $dFormat = new $df($pattern.toString());
            $lastDValue = $d;
            $numberFormat.applyPattern($dFormat.toPattern());
          }
          ${ev.value} = UTF8String.fromString($numberFormat.format(${typeHelper(num)}));
        } else {
          ${ev.value} = null;
          ${ev.isNull} = true;
        }
       """
    })
  }

  override def prettyName: String = "format_number"
}
