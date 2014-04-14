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

import scala.util.Random
import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * Implement the gibbs sampling LDA on spark. Input file's format is: docId \t date \t words(splited
 * by " "). Output the topic distribution of each file in "out/topicDistOnDoc" default and the topic
 * in "out/wordDistOnTopic" default.
 *
 * gist: https://gist.github.com/waleking/5477002
 */
object SparkGibbsLDA {

  /**
   * Print out topics, output topK words in each topic.
   */
  def topicsInfo(
      nkv: Array[Array[Int]],
      allWords: List[String],
      kTopic: Int,
      vSize: Int,
      topK: Int): String = {
    (0 until kTopic).map { k =>
      val res = mutable.StringBuilder.newBuilder
      val distOnTopic = for (v <- 0 until vSize) yield (v, nkv(k)(v))
      val sorted = distOnTopic.sortWith((tupleA, tupleB) => tupleA._2 > tupleB._2)
      res.append(s"topic $k:\n")
      for (j <- 0 until topK) {
        res.append(s"n(${allWords(sorted(j)._1)}) = ${sorted(j)._2}")
      }
      res.append("\n")
    }.reduceOption { case (lhs, rhs) =>
      lhs ++= rhs
    }.toString
  }

  def gibbsSampling(
      topicAssignArr: Array[(Int, Int)],
      nmk: Array[Int],
      nkv: Array[Array[Int]],
      nk: Array[Int],
      kTopic: Int,
      alpha: Double,
      vSize: Int,
      beta: Double): (Array[(Int, Int)], Array[Int]) = {
    val length = topicAssignArr.length

    for (i <- 0 until length) {
      val topic = topicAssignArr(i)._2
      val word = topicAssignArr(i)._1

      nmk(topic) = nmk(topic) - 1
      nkv(topic)(word) = nkv(topic)(word) - 1
      nk(topic) = nk(topic) - 1

      val topicDist = new Array[Double](kTopic)
      for (k <- 0 until kTopic) {
        topicDist(k) = (nmk(k).toDouble + alpha) * (nkv(k)(word) + beta) / (nk(k) + vSize * beta)
      }
      val newTopic = sampleFromMultinomial(topicDist)
      topicAssignArr(i) = (word, newTopic)

      nmk(newTopic) = nmk(newTopic) + 1
      nkv(newTopic)(word) = nkv(newTopic)(word) + 1
      nk(newTopic) = nk(newTopic) + 1
    }
    (topicAssignArr, nmk)
  }

  def updateNKV(
      wordsTopicReduced: List[((Int, Int), Int)],
      kTopic: Int,
      vSize: Int): Array[Array[Int]] = {
    val nkv = new Array[Array[Int]](kTopic)
    for (k <- 0 until kTopic) {
      nkv(k) = new Array[Int](vSize)
    }
    wordsTopicReduced.foreach{ t =>
      val word = t._1._1
      val topic = t._1._2
      val count = t._2
      nkv(topic)(word) = nkv(topic)(word) + count
    }
    nkv
  }

  def updateNK(wordsTopicReduced: List[((Int, Int), Int)], kTopic: Int, vSize: Int): Array[Int] = {
    val nk = new Array[Int](kTopic)
    wordsTopicReduced.foreach { t =>
      val topic = t._1._2
      val count = t._2
      nk(topic) = nk(topic) + count
    }
    nk
  }

  def sampleFromMultinomial(arrInput: Array[Double]): Int = {
    val rand = Random.nextDouble()
    val s = doubleArrayOps(arrInput).sum
    val arrNormalized = doubleArrayOps(arrInput).map { e => e / s }
    var localSum = 0.0
    val cumArr = doubleArrayOps(arrNormalized).map { dist =>
      localSum = localSum + dist
      localSum
    }
    doubleArrayOps(cumArr).indexWhere(cumDist => cumDist >= rand)
  }

  def restartSpark(sc: SparkContext, scMaster: String, remote: Boolean): SparkContext = {
    sc.stop()
    Thread.sleep(2000)
    if (remote) {
      new SparkContext(scMaster, "SparkLocalLDA", "./", Seq("job.jar"))
    } else {
      new SparkContext(scMaster, "SparkLocalLDA")
    }
  }

  def startSpark(remote: Boolean) = {
    if (remote) {
      val scMaster = "spark://db-PowerEdge-2970:7077" // e.g. local[4]
      val sparkContext = new SparkContext(scMaster, "SparkLocalLDA", "./", Seq("job.jar"))
      (scMaster, sparkContext)
    } else {
      val scMaster = "local[4]" // e.g. local[4]
      val sparkContext = new SparkContext(scMaster, "SparkLocalLDA")
      (scMaster, sparkContext)
    }
  }

  def saveDocTopicDist(
      documents: RDD[(Long, Array[(Int, Int)], Array[Int])],
      pathTopicDistOnDoc: String) {
    documents.map {
      case (docId, topicAssign, nmk) =>
        val docLen = topicAssign.length
        val probabilities = nmk.map(n => n / docLen.toDouble).toList
        (docId, probabilities)
    }.saveAsTextFile(pathTopicDistOnDoc)
  }

  def saveWordDistTopic(
      sc: SparkContext,
      nkv: Array[Array[Int]],
      nk: Array[Int],
      allWords: List[String],
      vSize: Int,
      topKWordsForDebug: Int,
      pathWordDistOnTopic: String) {
    val topicK = nkv.length

    //add topicid for array
    val nkvWithId = Array.fill(topicK) { (0, Array[Int](vSize)) }
    for (k <- 0 until topicK) {
      nkvWithId(k) = (k, nkv(k))
    }

    //output topKWordsForDebug words
    val res = sc.parallelize(nkvWithId).map { t =>
      val k = t._1
      val distOnTopic = for (v <- 0 until vSize) yield (v, t._2(v))
      val sorted = distOnTopic.sortWith((tupleA, tupleB) => tupleA._2 > tupleB._2)
      val topDist = {
        for (v <- 0 until topKWordsForDebug)
          yield (allWords(sorted(v)._1), sorted(v)._2.toDouble / nk(k).toDouble)
      }.toList
      (k, topDist)
    }
    res.saveAsTextFile(pathWordDistOnTopic)
  }

  def ldaMain(
      filename: String,
      kTopic: Int,
      alpha: Double,
      beta: Double,
      maxIter: Int,
      remote: Boolean,
      topKwordsForDebug: Int,
      pathTopicDistOnDoc: String,
      pathWordDistOnTopic: String) {

    //Step 1, start spark
    System.setProperty("file.encoding", "UTF-8")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    var (scMaster, sc) = startSpark(remote)

    //Step2, read files into HDFS
    val file = sc.textFile(filename)
    val rawFiles = file.map { line =>
      {
        val vs = line.split("\t")
        val words = vs(2).split(" ").toList
        (vs(0).toLong, words)
      }
    }.filter(_._2.length > 0)

    //Step3, build a dictionary for alphabet : wordIndexMap
    val allWords = rawFiles
      .flatMap(_._2.distinct)
      .map((_, 1))
      .reduceByKey(_+_)
      .map(_._1)
      .collect()
      .toList.sortWith(_ < _)

    val vSize = allWords.length

    val wordIndexMap = new mutable.HashMap[String, Int]()
    for (i <- 0 until allWords.length) {
      wordIndexMap(allWords(i)) = i
    }

    // Your want to use broadcast? Huh? -- Xusen
    val bWordIndexMap = wordIndexMap

    //Step4, init topic assignments for each word in the corpus
    val documents = rawFiles.map { t =>
      val docId = t._1
      val length = t._2.length
      val topicAssignArr = new Array[(Int, Int)](length)
      val nmk = new Array[Int](kTopic)
      for (i <- 0 until length) {
        val topic = Random.nextInt(kTopic)
        topicAssignArr(i) = (bWordIndexMap(t._2(i)), topic)
        nmk(topic) = nmk(topic) + 1
      }
      (docId, topicAssignArr, nmk)
    }.cache()

    var wordsTopicReduced = documents
      .flatMap(t => t._2)
      .map(t => (t, 1))
      .reduceByKey(_ + _)
      .collect()
      .toList

    //update nkv,nk
    var nkv = updateNKV(wordsTopicReduced, kTopic, vSize)
    var nkvGlobal = sc.broadcast(nkv)
    var nk = updateNK(wordsTopicReduced, kTopic, vSize)
    var nkGlobal = sc.broadcast(nk)
    //nk.foreach(println)

    //Step5, use gibbs sampling to infer the topic distribution in doc and estimate the parameter
    var iterativeInputDocuments = documents
    var updatedDocuments = iterativeInputDocuments
    for (iter <- 0 until maxIter) {
      iterativeInputDocuments.cache()
      updatedDocuments.cache()
      
      //broadcast the global data
      nkvGlobal = sc.broadcast(nkv)
      nkGlobal = sc.broadcast(nk)

      updatedDocuments = iterativeInputDocuments.map { case (docId, topicAssignArr, nmk) =>
        //gibbs sampling
        val (newTopicAssignArr, newNmk) = gibbsSampling(
          topicAssignArr,
          nmk,
          nkvGlobal.value,
          nkGlobal.value,
          kTopic,
          alpha,
          vSize,
          beta)
        (docId, newTopicAssignArr, newNmk)
      }
      
      wordsTopicReduced = updatedDocuments.
        flatMap(t => t._2).
        map(t => (t, 1)).
        reduceByKey(_ + _).
        collect().
        toList

      iterativeInputDocuments = updatedDocuments

      //update nkv,nk
      nkv = updateNKV(wordsTopicReduced, kTopic, vSize)
      nk = updateNK(wordsTopicReduced, kTopic, vSize)

      println(topicsInfo(nkvGlobal.value, allWords, kTopic, vSize, topKwordsForDebug))

      println("iteration " + iter + " finished")

      //restart spark to optimize the memory 
      if (iter % 20 == 0) {
        //save RDD temporally
        var pathDocument1=""
        var pathDocument2=""
        if(remote){
          pathDocument1="hdfs://192.9.200.175:9000/out/gibbsLDAtmp"
          pathDocument2="hdfs://192.9.200.175:9000/out/gibbsLDAtmp2"  
        }else{
          pathDocument1="out/gibbsLDAtmp"
          pathDocument2="out/gibbsLDAtmp2"
        }
        val storedDocuments1=iterativeInputDocuments
        storedDocuments1.persist(StorageLevel.DISK_ONLY)
        storedDocuments1.saveAsObjectFile(pathDocument1)
        val storedDocuments2=updatedDocuments
        storedDocuments2.persist(StorageLevel.DISK_ONLY)
        storedDocuments2.saveAsObjectFile(pathDocument2)        
        
        //restart Spark to solve the memory leak problem
        sc=restartSpark(sc, scMaster, remote)
        //as the restart of Spark, all of RDD are cleared
        //we need to read files in order to rebuild RDD
        iterativeInputDocuments=sc.objectFile(pathDocument1)
        updatedDocuments=sc.objectFile(pathDocument2)
      }
    }

    val resultDocuments = iterativeInputDocuments
    saveDocTopicDist(resultDocuments, pathTopicDistOnDoc)
    saveWordDistTopic(sc, nkv, nk, allWords, vSize, topKwordsForDebug, pathWordDistOnTopic)
  }

  def main(args: Array[String]) {
    val fileName="/tmp/ldasrc.txt"
    val kTopic = 10
    val alpha = 0.45
    val beta = 0.01
    val maxIterations = 1000
    val remote = true
    val topKWordsForDebug = 10
    val pathTopicDistOnDoc = "hdfs://192.9.200.175:9000/out/topicDistOnDoc"
    val pathWordDistOnTopic = "hdfs://192.9.200.175:9000/out/wordDistOnTopic"
    ldaMain(fileName,
      kTopic,
      alpha,
      beta,
      maxIterations,
      remote,
      topKWordsForDebug,
      pathTopicDistOnDoc,
      pathWordDistOnTopic)
  }
}