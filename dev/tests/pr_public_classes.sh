#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# This script follows the base format for testing pull requests against
# another branch and returning results to be published. More details can be
# found at dev/run-tests-jenkins.
#
# Arg1: The Github Pull Request Actual Commit
#+ known as `ghprbActualCommit` in `run-tests-jenkins`
# Arg2: The SHA1 hash
#+ known as `sha1` in `run-tests-jenkins`
#

# We diff master...$ghprbActualCommit because that gets us changes introduced in the PR
#+ and not anything else added to master since the PR was branched.

ghprbActualCommit="$1"
sha1="$2"

source_files=$(
  git diff master...$ghprbActualCommit --name-only  `# diff patch against master from branch point` \
    | grep -v -e "\/test"                               `# ignore files in test directories` \
    | grep -e "\.py$" -e "\.java$" -e "\.scala$"        `# include only code files` \
    | tr "\n" " "
)
new_public_classes=$(
  git diff master...$ghprbActualCommit ${source_files}      `# diff patch against master from branch point` \
    | grep "^\+"                              `# filter in only added lines` \
    | sed -r -e "s/^\+//g"                    `# remove the leading +` \
    | grep -e "trait " -e "class "            `# filter in lines with these key words` \
    | grep -e "{" -e "("                      `# filter in lines with these key words, too` \
    | grep -v -e "\@\@" -e "private"          `# exclude lines with these words` \
    | grep -v -e "^// " -e "^/\*" -e "^ \* "  `# exclude comment lines` \
    | sed -r -e "s/\{.*//g"                   `# remove from the { onwards` \
    | sed -r -e "s/\}//g"                     `# just in case, remove }; they mess the JSON` \
    | sed -r -e "s/\"/\\\\\"/g"               `# escape double quotes; they mess the JSON` \
    | sed -r -e "s/^(.*)$/\`\1\`/g"           `# surround with backticks for style` \
    | sed -r -e "s/^/  \* /g"                 `# prepend '  *' to start of line` \
    | sed -r -e "s/$/\\\n/g"                  `# append newline to end of line` \
    | tr -d "\n"                              `# remove actual LF characters`
)

if [ -z "$new_public_classes" ]; then
  echo " * This patch adds no public classes."
else
  public_classes_note=" * This patch adds the following public classes _(experimental)_:"
  echo "${public_classes_note}\n${new_public_classes}"
fi
