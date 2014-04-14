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

package org.apache.spark.mllib.clustering

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.model.Document

import breeze.util.Index
import chalk.text.tokenize.JavaWordTokenizer

/**
 * Helper methods to load, save and pre-process data used in ML Lib.
 */
object MLUtils extends Logging {

  /**
   * Load corpus from a given path. Terms and documents will be translated into integers, with a
   * term-integer map and a document-integer map.
   *
   * @param dir The path of corpus.
   * @param dirStopWords The path of stop words.
   * @return (RDD[Document], Term-integer map, doc-integer map)
   */
  def loadCorpus(
      sc: SparkContext,
      dir: String,
      minSplits: Int,
      dirStopWords: String = ""):
  (RDD[Document], Index[String], Index[String]) = {

    // Containers and indexers for terms and documents
    val termMap = Index[String]()
    val docMap = Index[String]()

    val stopWords =
      if (dirStopWords == "") {
        Set.empty[String]
      }
      else {
        sc.textFile(dirStopWords, minSplits).
          map(x => x.replaceAll( """(?m)\s+$""", "")).distinct().collect().toSet
      }
    val broadcastStopWord = sc.broadcast(stopWords)

    // Tokenize and filter terms
    val almostData = sc.wholeTextFiles(dir, minSplits).map { case (fileName, content) =>
      val tokens = JavaWordTokenizer(content)
        .filter(_(0).isLetter)
        .filter(!broadcastStopWord.value.contains(_))
      (fileName, tokens)
    }

    logInfo("Extracting file names...")
    almostData.map(_._1).collect().map(x => docMap.index(x))

    logInfo("Extracting terms...")
    almostData.flatMap(_._2).collect().map(x => termMap.index(x))

    println(termMap.size)
    println(docMap.size)

    val broadcastWordMap = sc.broadcast(termMap)
    val broadcastDocMap = sc.broadcast(docMap)

    logInfo("Translate documents of terms into integers...")
    val data = almostData.map { case (fileName, tokens) =>
      val fileIdx = broadcastDocMap.value.index(fileName)
      val translatedContent = tokens.map(broadcastWordMap.value.index)
      Document(fileIdx, translatedContent)
    }.cache()

    (data, termMap, docMap)
  }
}
