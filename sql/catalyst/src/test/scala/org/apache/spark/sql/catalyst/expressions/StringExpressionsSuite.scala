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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._


class StringExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("concat") {
    def testConcat(inputs: String*): Unit = {
      val expected = if (inputs.contains(null)) null else inputs.mkString
      checkEvaluation(Concat(inputs.map(Literal.create(_, StringType))), expected, EmptyRow)
    }

    testConcat()
    testConcat(null)
    testConcat("")
    testConcat("ab")
    testConcat("a", "b")
    testConcat("a", "b", "C")
    testConcat("a", null, "C")
    testConcat("a", null, null)
    testConcat(null, null, null)

    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    testConcat("数据", null, "砖头")
    // scalastyle:on
  }

  test("concat_ws") {
    def testConcatWs(expected: String, sep: String, inputs: Any*): Unit = {
      val inputExprs = inputs.map {
        case s: Seq[_] => Literal.create(s, ArrayType(StringType))
        case null => Literal.create(null, StringType)
        case s: String => Literal.create(s, StringType)
      }
      val sepExpr = Literal.create(sep, StringType)
      checkEvaluation(ConcatWs(sepExpr +: inputExprs), expected, EmptyRow)
    }

    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    testConcatWs(null, null)
    testConcatWs(null, null, "a", "b")
    testConcatWs("", "")
    testConcatWs("ab", "哈哈", "ab")
    testConcatWs("a哈哈b", "哈哈", "a", "b")
    testConcatWs("a哈哈b", "哈哈", "a", null, "b")
    testConcatWs("a哈哈b哈哈c", "哈哈", null, "a", null, "b", "c")

    testConcatWs("ab", "哈哈", Seq("ab"))
    testConcatWs("a哈哈b", "哈哈", Seq("a", "b"))
    testConcatWs("a哈哈b哈哈c哈哈d", "哈哈", Seq("a", null, "b"), null, "c", Seq(null, "d"))
    testConcatWs("a哈哈b哈哈c", "哈哈", Seq("a", null, "b"), null, "c", Seq.empty[String])
    testConcatWs("a哈哈b哈哈c", "哈哈", Seq("a", null, "b"), null, "c", Seq[String](null))
    // scalastyle:on
  }

  test("StringComparison") {
    val row = create_row("abc", null)
    val c1 = 'a.string.at(0)
    val c2 = 'a.string.at(1)

    checkEvaluation(c1 contains "b", true, row)
    checkEvaluation(c1 contains "x", false, row)
    checkEvaluation(c2 contains "b", null, row)
    checkEvaluation(c1 contains Literal.create(null, StringType), null, row)

    checkEvaluation(c1 startsWith "a", true, row)
    checkEvaluation(c1 startsWith "b", false, row)
    checkEvaluation(c2 startsWith "a", null, row)
    checkEvaluation(c1 startsWith Literal.create(null, StringType), null, row)

    checkEvaluation(c1 endsWith "c", true, row)
    checkEvaluation(c1 endsWith "b", false, row)
    checkEvaluation(c2 endsWith "b", null, row)
    checkEvaluation(c1 endsWith Literal.create(null, StringType), null, row)
  }

  test("Substring") {
    val row = create_row("example", "example".toArray.map(_.toByte))

    val s = 'a.string.at(0)

    // substring from zero position with less-than-full length
    checkEvaluation(
      Substring(s, Literal.create(0, IntegerType), Literal.create(2, IntegerType)), "ex", row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(2, IntegerType)), "ex", row)

    // substring from zero position with full length
    checkEvaluation(
      Substring(s, Literal.create(0, IntegerType), Literal.create(7, IntegerType)), "example", row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(7, IntegerType)), "example", row)

    // substring from zero position with greater-than-full length
    checkEvaluation(Substring(s, Literal.create(0, IntegerType), Literal.create(100, IntegerType)),
      "example", row)
    checkEvaluation(Substring(s, Literal.create(1, IntegerType), Literal.create(100, IntegerType)),
      "example", row)

    // substring from nonzero position with less-than-full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(2, IntegerType)),
      "xa", row)

    // substring from nonzero position with full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(6, IntegerType)),
      "xample", row)

    // substring from nonzero position with greater-than-full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(100, IntegerType)),
      "xample", row)

    // zero-length substring (within string bounds)
    checkEvaluation(Substring(s, Literal.create(0, IntegerType), Literal.create(0, IntegerType)),
      "", row)

    // zero-length substring (beyond string bounds)
    checkEvaluation(Substring(s, Literal.create(100, IntegerType), Literal.create(4, IntegerType)),
      "", row)

    // substring(null, _, _) -> null
    checkEvaluation(Substring(s, Literal.create(100, IntegerType), Literal.create(4, IntegerType)),
      null, create_row(null))

    // substring(_, null, _) -> null
    checkEvaluation(Substring(s, Literal.create(null, IntegerType), Literal.create(4, IntegerType)),
      null, row)

    // substring(_, _, null) -> null
    checkEvaluation(
      Substring(s, Literal.create(100, IntegerType), Literal.create(null, IntegerType)),
      null,
      row)

    // 2-arg substring from zero position
    checkEvaluation(
      Substring(s, Literal.create(0, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "example",
      row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "example",
      row)

    // 2-arg substring from nonzero position
    checkEvaluation(
      Substring(s, Literal.create(2, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "xample",
      row)

    val s_notNull = 'a.string.notNull.at(0)

    assert(Substring(s, Literal.create(0, IntegerType), Literal.create(2, IntegerType)).nullable
      === true)
    assert(
      Substring(s_notNull, Literal.create(0, IntegerType), Literal.create(2, IntegerType)).nullable
        === false)
    assert(Substring(s_notNull,
      Literal.create(null, IntegerType), Literal.create(2, IntegerType)).nullable === true)
    assert(Substring(s_notNull,
      Literal.create(0, IntegerType), Literal.create(null, IntegerType)).nullable === true)

    checkEvaluation(s.substr(0, 2), "ex", row)
    checkEvaluation(s.substr(0), "example", row)
    checkEvaluation(s.substring(0, 2), "ex", row)
    checkEvaluation(s.substring(0), "example", row)

    val bytes = Array[Byte](1, 2, 3, 4)
    checkEvaluation(Substring(bytes, 0, 2), Array[Byte](1, 2))
    checkEvaluation(Substring(bytes, 1, 2), Array[Byte](1, 2))
    checkEvaluation(Substring(bytes, 2, 2), Array[Byte](2, 3))
    checkEvaluation(Substring(bytes, 3, 2), Array[Byte](3, 4))
    checkEvaluation(Substring(bytes, 4, 2), Array[Byte](4))
    checkEvaluation(Substring(bytes, 8, 2), Array[Byte]())
    checkEvaluation(Substring(bytes, -1, 2), Array[Byte](4))
    checkEvaluation(Substring(bytes, -2, 2), Array[Byte](3, 4))
    checkEvaluation(Substring(bytes, -3, 2), Array[Byte](2, 3))
    checkEvaluation(Substring(bytes, -4, 2), Array[Byte](1, 2))
    checkEvaluation(Substring(bytes, -5, 2), Array[Byte](1))
    checkEvaluation(Substring(bytes, -8, 2), Array[Byte]())
  }

  test("string substring_index function") {
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(3)), "www.apache.org")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(2)), "www.apache")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(1)), "www")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(0)), "")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(-3)), "www.apache.org")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(-2)), "apache.org")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(-1)), "org")
    checkEvaluation(
      SubstringIndex(Literal(""), Literal("."), Literal(-2)), "")
    checkEvaluation(
      SubstringIndex(Literal.create(null, StringType), Literal("."), Literal(-2)), null)
    checkEvaluation(SubstringIndex(
        Literal("www.apache.org"), Literal.create(null, StringType), Literal(-2)), null)
    // non ascii chars
    // scalastyle:off
    checkEvaluation(
      SubstringIndex(Literal("大千世界大千世界"), Literal( "千"), Literal(2)), "大千世界大")
    // scalastyle:on
    checkEvaluation(
      SubstringIndex(Literal("www||apache||org"), Literal( "||"), Literal(2)), "www||apache")
  }

  test("LIKE literal Regular Expression") {
    checkEvaluation(Literal.create(null, StringType).like("a"), null)
    checkEvaluation(Literal.create("a", StringType).like(Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create(null, StringType).like(Literal.create(null, StringType)), null)
    checkEvaluation(
      Literal.create("a", StringType).like(NonFoldableLiteral.create("a", StringType)), true)
    checkEvaluation(
      Literal.create("a", StringType).like(NonFoldableLiteral.create(null, StringType)), null)
    checkEvaluation(
      Literal.create(null, StringType).like(NonFoldableLiteral.create("a", StringType)), null)
    checkEvaluation(
      Literal.create(null, StringType).like(NonFoldableLiteral.create(null, StringType)), null)

    checkEvaluation("abdef" like "abdef", true)
    checkEvaluation("a_%b" like "a\\__b", true)
    checkEvaluation("addb" like "a_%b", true)
    checkEvaluation("addb" like "a\\__b", false)
    checkEvaluation("addb" like "a%\\%b", false)
    checkEvaluation("a_%b" like "a%\\%b", true)
    checkEvaluation("addb" like "a%", true)
    checkEvaluation("addb" like "**", false)
    checkEvaluation("abc" like "a%", true)
    checkEvaluation("abc"  like "b%", false)
    checkEvaluation("abc"  like "bc%", false)
    checkEvaluation("a\nb" like "a_b", true)
    checkEvaluation("ab" like "a%b", true)
    checkEvaluation("a\nb" like "a%b", true)
  }

  test("LIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abcd" like regEx, null, create_row(null))
    checkEvaluation("abdef" like regEx, true, create_row("abdef"))
    checkEvaluation("a_%b" like regEx, true, create_row("a\\__b"))
    checkEvaluation("addb" like regEx, true, create_row("a_%b"))
    checkEvaluation("addb" like regEx, false, create_row("a\\__b"))
    checkEvaluation("addb" like regEx, false, create_row("a%\\%b"))
    checkEvaluation("a_%b" like regEx, true, create_row("a%\\%b"))
    checkEvaluation("addb" like regEx, true, create_row("a%"))
    checkEvaluation("addb" like regEx, false, create_row("**"))
    checkEvaluation("abc" like regEx, true, create_row("a%"))
    checkEvaluation("abc" like regEx, false, create_row("b%"))
    checkEvaluation("abc" like regEx, false, create_row("bc%"))
    checkEvaluation("a\nb" like regEx, true, create_row("a_b"))
    checkEvaluation("ab" like regEx, true, create_row("a%b"))
    checkEvaluation("a\nb" like regEx, true, create_row("a%b"))

    checkEvaluation(Literal.create(null, StringType) like regEx, null, create_row("bc%"))
  }

  test("RLIKE literal Regular Expression") {
    checkEvaluation(Literal.create(null, StringType) rlike "abdef", null)
    checkEvaluation("abdef" rlike Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, StringType) rlike Literal.create(null, StringType), null)
    checkEvaluation("abdef" rlike NonFoldableLiteral.create("abdef", StringType), true)
    checkEvaluation("abdef" rlike NonFoldableLiteral.create(null, StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) rlike NonFoldableLiteral.create("abdef", StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) rlike NonFoldableLiteral.create(null, StringType), null)

    checkEvaluation("abdef" rlike "abdef", true)
    checkEvaluation("abbbbc" rlike "a.*c", true)

    checkEvaluation("fofo" rlike "^fo", true)
    checkEvaluation("fo\no" rlike "^fo\no$", true)
    checkEvaluation("Bn" rlike "^Ba*n", true)
    checkEvaluation("afofo" rlike "fo", true)
    checkEvaluation("afofo" rlike "^fo", false)
    checkEvaluation("Baan" rlike "^Ba?n", false)
    checkEvaluation("axe" rlike "pi|apa", false)
    checkEvaluation("pip" rlike "^(pi)*$", false)

    checkEvaluation("abc"  rlike "^ab", true)
    checkEvaluation("abc"  rlike "^bc", false)
    checkEvaluation("abc"  rlike "^ab", true)
    checkEvaluation("abc"  rlike "^bc", false)

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike "**")
    }
  }

  test("RLIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abdef" rlike regEx, true, create_row("abdef"))
    checkEvaluation("abbbbc" rlike regEx, true, create_row("a.*c"))
    checkEvaluation("fofo" rlike regEx, true, create_row("^fo"))
    checkEvaluation("fo\no" rlike regEx, true, create_row("^fo\no$"))
    checkEvaluation("Bn" rlike regEx, true, create_row("^Ba*n"))

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike regEx, create_row("**"))
    }
  }

  test("ascii for string") {
    val a = 'a.string.at(0)
    checkEvaluation(Ascii(Literal("efg")), 101, create_row("abdef"))
    checkEvaluation(Ascii(a), 97, create_row("abdef"))
    checkEvaluation(Ascii(a), 0, create_row(""))
    checkEvaluation(Ascii(a), null, create_row(null))
    checkEvaluation(Ascii(Literal.create(null, StringType)), null, create_row("abdef"))
  }

  test("base64/unbase64 for string") {
    val a = 'a.string.at(0)
    val b = 'b.binary.at(0)
    val bytes = Array[Byte](1, 2, 3, 4)

    checkEvaluation(Base64(Literal(bytes)), "AQIDBA==", create_row("abdef"))
    checkEvaluation(Base64(UnBase64(Literal("AQIDBA=="))), "AQIDBA==", create_row("abdef"))
    checkEvaluation(Base64(UnBase64(Literal(""))), "", create_row("abdef"))
    checkEvaluation(Base64(UnBase64(Literal.create(null, StringType))), null, create_row("abdef"))
    checkEvaluation(Base64(UnBase64(a)), "AQIDBA==", create_row("AQIDBA=="))

    checkEvaluation(Base64(b), "AQIDBA==", create_row(bytes))
    checkEvaluation(Base64(b), "", create_row(Array[Byte]()))
    checkEvaluation(Base64(b), null, create_row(null))
    checkEvaluation(Base64(Literal.create(null, BinaryType)), null, create_row("abdef"))

    checkEvaluation(UnBase64(a), null, create_row(null))
    checkEvaluation(UnBase64(Literal.create(null, StringType)), null, create_row("abdef"))
  }

  test("encode/decode for string") {
    val a = 'a.string.at(0)
    val b = 'b.binary.at(0)
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    checkEvaluation(
      Decode(Encode(Literal("大千世界"), Literal("UTF-16LE")), Literal("UTF-16LE")), "大千世界")
    checkEvaluation(
      Decode(Encode(a, Literal("utf-8")), Literal("utf-8")), "大千世界", create_row("大千世界"))
    checkEvaluation(
      Decode(Encode(a, Literal("utf-8")), Literal("utf-8")), "", create_row(""))
    // scalastyle:on
    checkEvaluation(Encode(a, Literal("utf-8")), null, create_row(null))
    checkEvaluation(Encode(Literal.create(null, StringType), Literal("utf-8")), null)
    checkEvaluation(Encode(a, Literal.create(null, StringType)), null, create_row(""))

    checkEvaluation(Decode(b, Literal("utf-8")), null, create_row(null))
    checkEvaluation(Decode(Literal.create(null, BinaryType), Literal("utf-8")), null)
    checkEvaluation(Decode(b, Literal.create(null, StringType)), null, create_row(null))
  }

  test("initcap unit test") {
    checkEvaluation(InitCap(Literal.create(null, StringType)), null)
    checkEvaluation(InitCap(Literal("a b")), "A B")
    checkEvaluation(InitCap(Literal(" a")), " A")
    checkEvaluation(InitCap(Literal("the test")), "The Test")
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    checkEvaluation(InitCap(Literal("世界")), "世界")
    // scalastyle:on
  }


  test("Levenshtein distance") {
    checkEvaluation(Levenshtein(Literal.create(null, StringType), Literal("")), null)
    checkEvaluation(Levenshtein(Literal(""), Literal.create(null, StringType)), null)
    checkEvaluation(Levenshtein(Literal(""), Literal("")), 0)
    checkEvaluation(Levenshtein(Literal("abc"), Literal("abc")), 0)
    checkEvaluation(Levenshtein(Literal("kitten"), Literal("sitting")), 3)
    checkEvaluation(Levenshtein(Literal("frog"), Literal("fog")), 1)
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    checkEvaluation(Levenshtein(Literal("千世"), Literal("fog")), 3)
    checkEvaluation(Levenshtein(Literal("世界千世"), Literal("大a界b")), 4)
    // scalastyle:on
  }

  test("soundex unit test") {
    checkEvaluation(SoundEx(Literal("ZIN")), "Z500")
    checkEvaluation(SoundEx(Literal("SU")), "S000")
    checkEvaluation(SoundEx(Literal("")), "")
    checkEvaluation(SoundEx(Literal.create(null, StringType)), null)

    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    checkEvaluation(SoundEx(Literal("测试")), "测试")
    checkEvaluation(SoundEx(Literal("Tschüss")), "T220")
    // scalastyle:on
    checkEvaluation(SoundEx(Literal("zZ")), "Z000", create_row("s8"))
    checkEvaluation(SoundEx(Literal("RAGSSEEESSSVEEWE")), "R221")
    checkEvaluation(SoundEx(Literal("Ashcraft")), "A261")
    checkEvaluation(SoundEx(Literal("Aswcraft")), "A261")
    checkEvaluation(SoundEx(Literal("Tymczak")), "T522")
    checkEvaluation(SoundEx(Literal("Pfister")), "P236")
    checkEvaluation(SoundEx(Literal("Miller")), "M460")
    checkEvaluation(SoundEx(Literal("Peterson")), "P362")
    checkEvaluation(SoundEx(Literal("Peters")), "P362")
    checkEvaluation(SoundEx(Literal("Auerbach")), "A612")
    checkEvaluation(SoundEx(Literal("Uhrbach")), "U612")
    checkEvaluation(SoundEx(Literal("Moskowitz")), "M232")
    checkEvaluation(SoundEx(Literal("Moskovitz")), "M213")
    checkEvaluation(SoundEx(Literal("relyheewsgeessg")), "R422")
    checkEvaluation(SoundEx(Literal("!!")), "!!")
  }

  test("translate") {
    checkEvaluation(
      StringTranslate(Literal("translate"), Literal("rnlt"), Literal("123")), "1a2s3ae")
    checkEvaluation(StringTranslate(Literal("translate"), Literal(""), Literal("123")), "translate")
    checkEvaluation(StringTranslate(Literal("translate"), Literal("rnlt"), Literal("")), "asae")
    // test for multiple mapping
    checkEvaluation(StringTranslate(Literal("abcd"), Literal("aba"), Literal("123")), "12cd")
    checkEvaluation(StringTranslate(Literal("abcd"), Literal("aba"), Literal("12")), "12cd")
    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(StringTranslate(Literal("花花世界"), Literal("花界"), Literal("ab")), "aa世b")
    // scalastyle:on
  }

  test("TRIM/LTRIM/RTRIM") {
    val s = 'a.string.at(0)
    checkEvaluation(StringTrim(Literal(" aa  ")), "aa", create_row(" abdef "))
    checkEvaluation(StringTrim(s), "abdef", create_row(" abdef "))

    checkEvaluation(StringTrimLeft(Literal(" aa  ")), "aa  ", create_row(" abdef "))
    checkEvaluation(StringTrimLeft(s), "abdef ", create_row(" abdef "))

    checkEvaluation(StringTrimRight(Literal(" aa  ")), " aa", create_row(" abdef "))
    checkEvaluation(StringTrimRight(s), " abdef", create_row(" abdef "))

    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(StringTrimRight(s), "  花花世界", create_row("  花花世界 "))
    checkEvaluation(StringTrimLeft(s), "花花世界 ", create_row("  花花世界 "))
    checkEvaluation(StringTrim(s), "花花世界", create_row("  花花世界 "))
    // scalastyle:on
    checkEvaluation(StringTrim(Literal.create(null, StringType)), null)
    checkEvaluation(StringTrimLeft(Literal.create(null, StringType)), null)
    checkEvaluation(StringTrimRight(Literal.create(null, StringType)), null)
  }

  test("FORMAT") {
    checkEvaluation(FormatString(Literal("aa%d%s"), Literal(123), Literal("a")), "aa123a")
    checkEvaluation(FormatString(Literal("aa")), "aa", create_row(null))
    checkEvaluation(FormatString(Literal("aa%d%s"), Literal(123), Literal("a")), "aa123a")
    checkEvaluation(FormatString(Literal("aa%d%s"), 12, "cc"), "aa12cc")

    checkEvaluation(FormatString(Literal.create(null, StringType), 12, "cc"), null)
    checkEvaluation(
      FormatString(Literal("aa%d%s"), Literal.create(null, IntegerType), "cc"), "aanullcc")
    checkEvaluation(
      FormatString(Literal("aa%d%s"), 12, Literal.create(null, StringType)), "aa12null")
  }

  test("INSTR") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.string.at(1)
    val s3 = 'c.string.at(2)
    val row1 = create_row("aaads", "aa", "zz")

    checkEvaluation(StringInstr(Literal("aaads"), Literal("aa")), 1, row1)
    checkEvaluation(StringInstr(Literal("aaads"), Literal("de")), 0, row1)
    checkEvaluation(StringInstr(Literal.create(null, StringType), Literal("de")), null, row1)
    checkEvaluation(StringInstr(Literal("aaads"), Literal.create(null, StringType)), null, row1)

    checkEvaluation(StringInstr(s1, s2), 1, row1)
    checkEvaluation(StringInstr(s1, s3), 0, row1)

    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(StringInstr(s1, s2), 3, create_row("花花世界", "世界"))
    checkEvaluation(StringInstr(s1, s2), 1, create_row("花花世界", "花"))
    checkEvaluation(StringInstr(s1, s2), 0, create_row("花花世界", "小"))
    // scalastyle:on
  }

  test("LOCATE") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.string.at(1)
    val s3 = 'c.string.at(2)
    val s4 = 'd.int.at(3)
    val row1 = create_row("aaads", "aa", "zz", 1)
    val row2 = create_row(null, "aa", "zz", 0)
    val row3 = create_row("aaads", null, "zz", 0)
    val row4 = create_row(null, null, null, 0)

    checkEvaluation(new StringLocate(Literal("aa"), Literal("aaads")), 1, row1)
    checkEvaluation(StringLocate(Literal("aa"), Literal("aaads"), Literal(1)), 2, row1)
    checkEvaluation(StringLocate(Literal("aa"), Literal("aaads"), Literal(2)), 0, row1)
    checkEvaluation(new StringLocate(Literal("de"), Literal("aaads")), 0, row1)
    checkEvaluation(StringLocate(Literal("de"), Literal("aaads"), 1), 0, row1)

    checkEvaluation(new StringLocate(s2, s1), 1, row1)
    checkEvaluation(StringLocate(s2, s1, s4), 2, row1)
    checkEvaluation(new StringLocate(s3, s1), 0, row1)
    checkEvaluation(StringLocate(s3, s1, Literal.create(null, IntegerType)), 0, row1)
    checkEvaluation(new StringLocate(s2, s1), null, row2)
    checkEvaluation(new StringLocate(s2, s1), null, row3)
    checkEvaluation(new StringLocate(s2, s1, Literal.create(null, IntegerType)), 0, row4)
  }

  test("LPAD/RPAD") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.int.at(1)
    val s3 = 'c.string.at(2)
    val row1 = create_row("hi", 5, "??")
    val row2 = create_row("hi", 1, "?")
    val row3 = create_row(null, 1, "?")
    val row4 = create_row("hi", null, "?")
    val row5 = create_row("hi", 1, null)

    checkEvaluation(StringLPad(Literal("hi"), Literal(5), Literal("??")), "???hi", row1)
    checkEvaluation(StringLPad(Literal("hi"), Literal(1), Literal("??")), "h", row1)
    checkEvaluation(StringLPad(s1, s2, s3), "???hi", row1)
    checkEvaluation(StringLPad(s1, s2, s3), "h", row2)
    checkEvaluation(StringLPad(s1, s2, s3), null, row3)
    checkEvaluation(StringLPad(s1, s2, s3), null, row4)
    checkEvaluation(StringLPad(s1, s2, s3), null, row5)

    checkEvaluation(StringRPad(Literal("hi"), Literal(5), Literal("??")), "hi???", row1)
    checkEvaluation(StringRPad(Literal("hi"), Literal(1), Literal("??")), "h", row1)
    checkEvaluation(StringRPad(s1, s2, s3), "hi???", row1)
    checkEvaluation(StringRPad(s1, s2, s3), "h", row2)
    checkEvaluation(StringRPad(s1, s2, s3), null, row3)
    checkEvaluation(StringRPad(s1, s2, s3), null, row4)
    checkEvaluation(StringRPad(s1, s2, s3), null, row5)
  }

  test("REPEAT") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.int.at(1)
    val row1 = create_row("hi", 2)
    val row2 = create_row(null, 1)

    checkEvaluation(StringRepeat(Literal("hi"), Literal(2)), "hihi", row1)
    checkEvaluation(StringRepeat(Literal("hi"), Literal(-1)), "", row1)
    checkEvaluation(StringRepeat(s1, s2), "hihi", row1)
    checkEvaluation(StringRepeat(s1, s2), null, row2)
  }

  test("REVERSE") {
    val s = 'a.string.at(0)
    val row1 = create_row("abccc")
    checkEvaluation(StringReverse(Literal("abccc")), "cccba", row1)
    checkEvaluation(StringReverse(s), "cccba", row1)
    checkEvaluation(StringReverse(Literal.create(null, StringType)), null, row1)
  }

  test("SPACE") {
    val s1 = 'b.int.at(0)
    val row1 = create_row(2)
    val row2 = create_row(null)

    checkEvaluation(StringSpace(Literal(2)), "  ", row1)
    checkEvaluation(StringSpace(Literal(-1)), "", row1)
    checkEvaluation(StringSpace(Literal(0)), "", row1)
    checkEvaluation(StringSpace(s1), "  ", row1)
    checkEvaluation(StringSpace(s1), null, row2)
  }

  test("RegexReplace") {
    val row1 = create_row("100-200", "(\\d+)", "num")
    val row2 = create_row("100-200", "(\\d+)", "###")
    val row3 = create_row("100-200", "(-)", "###")
    val row4 = create_row(null, "(\\d+)", "###")
    val row5 = create_row("100-200", null, "###")
    val row6 = create_row("100-200", "(-)", null)

    val s = 's.string.at(0)
    val p = 'p.string.at(1)
    val r = 'r.string.at(2)

    val expr = RegExpReplace(s, p, r)
    checkEvaluation(expr, "num-num", row1)
    checkEvaluation(expr, "###-###", row2)
    checkEvaluation(expr, "100###200", row3)
    checkEvaluation(expr, null, row4)
    checkEvaluation(expr, null, row5)
    checkEvaluation(expr, null, row6)
  }

  test("RegexExtract") {
    val row1 = create_row("100-200", "(\\d+)-(\\d+)", 1)
    val row2 = create_row("100-200", "(\\d+)-(\\d+)", 2)
    val row3 = create_row("100-200", "(\\d+).*", 1)
    val row4 = create_row("100-200", "([a-z])", 1)
    val row5 = create_row(null, "([a-z])", 1)
    val row6 = create_row("100-200", null, 1)
    val row7 = create_row("100-200", "([a-z])", null)

    val s = 's.string.at(0)
    val p = 'p.string.at(1)
    val r = 'r.int.at(2)

    val expr = RegExpExtract(s, p, r)
    checkEvaluation(expr, "100", row1)
    checkEvaluation(expr, "200", row2)
    checkEvaluation(expr, "100", row3)
    checkEvaluation(expr, "", row4) // will not match anything, empty string get
    checkEvaluation(expr, null, row5)
    checkEvaluation(expr, null, row6)
    checkEvaluation(expr, null, row7)

    val expr1 = new RegExpExtract(s, p)
    checkEvaluation(expr1, "100", row1)
  }

  test("SPLIT") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.string.at(1)
    val row1 = create_row("aa2bb3cc", "[1-9]+")
    val row2 = create_row(null, "[1-9]+")
    val row3 = create_row("aa2bb3cc", null)

    checkEvaluation(
      StringSplit(Literal("aa2bb3cc"), Literal("[1-9]+")), Seq("aa", "bb", "cc"), row1)
    checkEvaluation(
      StringSplit(s1, s2), Seq("aa", "bb", "cc"), row1)
    checkEvaluation(StringSplit(s1, s2), null, row2)
    checkEvaluation(StringSplit(s1, s2), null, row3)
  }

  test("length for string / binary") {
    val a = 'a.string.at(0)
    val b = 'b.binary.at(0)
    val bytes = Array[Byte](1, 2, 3, 1, 2)
    val string = "abdef"

    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(Length(Literal("a花花c")), 4, create_row(string))
    // scalastyle:on
    checkEvaluation(Length(Literal(bytes)), 5, create_row(Array[Byte]()))

    checkEvaluation(Length(a), 5, create_row(string))
    checkEvaluation(Length(b), 5, create_row(bytes))

    checkEvaluation(Length(a), 0, create_row(""))
    checkEvaluation(Length(b), 0, create_row(Array[Byte]()))

    checkEvaluation(Length(a), null, create_row(null))
    checkEvaluation(Length(b), null, create_row(null))

    checkEvaluation(Length(Literal.create(null, StringType)), null, create_row(string))
    checkEvaluation(Length(Literal.create(null, BinaryType)), null, create_row(bytes))
  }

  test("format_number / FormatNumber") {
    checkEvaluation(FormatNumber(Literal(4.asInstanceOf[Byte]), Literal(3)), "4.000")
    checkEvaluation(FormatNumber(Literal(4.asInstanceOf[Short]), Literal(3)), "4.000")
    checkEvaluation(FormatNumber(Literal(4.0f), Literal(3)), "4.000")
    checkEvaluation(FormatNumber(Literal(4), Literal(3)), "4.000")
    checkEvaluation(FormatNumber(Literal(12831273.23481d), Literal(3)), "12,831,273.235")
    checkEvaluation(FormatNumber(Literal(12831273.83421d), Literal(0)), "12,831,274")
    checkEvaluation(FormatNumber(Literal(123123324123L), Literal(3)), "123,123,324,123.000")
    checkEvaluation(FormatNumber(Literal(123123324123L), Literal(-1)), null)
    checkEvaluation(
      FormatNumber(
        Literal(Decimal(123123324123L) * Decimal(123123.21234d)), Literal(4)),
      "15,159,339,180,002,773.2778")
    checkEvaluation(FormatNumber(Literal.create(null, IntegerType), Literal(3)), null)
    checkEvaluation(FormatNumber(Literal.create(null, NullType), Literal(3)), null)
  }

  test("find in set") {
    checkEvaluation(
      FindInSet(Literal.create(null, StringType), Literal.create(null, StringType)), null)
    checkEvaluation(FindInSet(Literal("ab"), Literal.create(null, StringType)), null)
    checkEvaluation(FindInSet(Literal.create(null, StringType), Literal("abc,b,ab,c,def")), null)
    checkEvaluation(FindInSet(Literal("ab"), Literal("abc,b,ab,c,def")), 3)
    checkEvaluation(FindInSet(Literal("abf"), Literal("abc,b,ab,c,def")), 0)
    checkEvaluation(FindInSet(Literal("ab,"), Literal("abc,b,ab,c,def")), 0)
  }
}
