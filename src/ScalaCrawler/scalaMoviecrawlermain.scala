package ScalaCrawler

import java.io.{File, PrintWriter}
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try} 

object scalaMoviecrawlermain {
val URL = "https://movie.douban.com/tag/%s?start=%d&type=T"
val MovieOutputRootDir = new File("MovieOutput").mkdir()
  //訪問的連結
  //需要抓取的電影標籤和頁數
  val tags = Map(
    "经典" -> 4, //tag，頁數
    "爱情" -> 4,
    "动作" -> 4,
    "剧情" -> 4,
    "悬疑" -> 4,
    "文艺" -> 4,
    "搞笑" -> 4,
    "战争" -> 4
  )

  //解析URL透過Get方法索回傳的html程式碼
  def parseDoc(doc: Document, movies: ConcurrentHashMap[String, String]) = {
    var count = 0
    for (elem <- doc.select("tr.item")) {
      movies.put(elem.select("a.nbg").attr("title"), elem.select("a.nbg").attr("title") + "\t" //標題
        + elem.select("a.nbg").attr("href") + "\t" //豆瓣連結
        // +elem.select("p.pl").html+"\t"//簡介
        + elem.select("span.rating_nums").html + "\t" //評分
        + elem.select("span.pl").html //評論數
      )
      count += 1
    }
    count
  }

  //用於記錄總數、和失敗次數
  val sum, fail: AtomicInteger = new AtomicInteger(0)
  /**
    *  當出現異常時10秒後重試,異常重複100次
    * @param delay：延遲時間
    * @param url：抓取的網址
    * @param movies：存取抓到的内容
    */
  def requestGetUrl(times: Int = 100, delay: Long = 10000)(url: String, movies: ConcurrentHashMap[String, String]): Unit = {
    Try(Jsoup.connect(url).get()) match {//使用try來判斷是否成功和失敗對網頁進行抓取
      case Failure(e) =>
        if (times != 0) {
          println(e.getMessage)
          fail.addAndGet(1)
          Thread.sleep(delay)
          requestGetUrl(times - 1, delay)(url, movies)
        } else throw e
      case Success(doc) =>
        val count = parseDoc(doc, movies);
        if (count == 0) {
          Thread.sleep(delay);
          requestGetUrl(times - 1, delay)(url, movies)
        }
        sum.addAndGet(count);
    }
  }

  /**
    * 多線程抓取
    * @param url:原始的Url
    * @param tag：電影標籤(分類)
    * @param maxPage：頁數
    * @param threadNum：執行緒數
    * @param movies：多執行緒HashMap(ConcurrentHashMap)儲存抓取到的內容
    */
  def concurrentCrawler(url: String, tag: String, maxPage: Int, threadNum: Int, movies: ConcurrentHashMap[String, String]) = {
    val loopPar = (0 to maxPage).par
    loopPar.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(threadNum)) //設置多執行緒數
    loopPar.foreach(i => requestGetUrl()(url.format(URLEncoder.encode(tag, "UTF-8"), 20 * i), movies)) // 利用并发集合多线程同步抓取:遍历所有页
    SortAndsaveFile(tag, movies)
  }

  //直接输出
  def saveFile(file: String, movies: ConcurrentHashMap[String, String]) = {
    val writer = new PrintWriter(new File("MovieOutput/" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" + file ++ ".txt"))
    for ((_, value) <- movies){
      writer.println(value)
    }
    writer.close()
  }

  // 依照電影的評價排序並輸出到文件(txt檔)
  def SortAndsaveFile(file: String, movies: ConcurrentHashMap[String, String]) = {
    val writer = new PrintWriter(new File("MovieOutput/" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" + file ++ ".txt"))
    val col = new ArrayBuffer[String]();
    for ((_, value) <- movies)
      col += value;
    val sort = col.sortWith(
      (o1, o2) => {
        val s1 = o1.split("\t")(2);
        val s2 = o2.split("\t")(2);
        if (s1 == null || s2 == null || s1.isEmpty || s2.isEmpty) {
          true
        } else {
          s1.toFloat > s2.toFloat
        }
      }
    )
    sort.foreach(writer.println(_))
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val Thread_Num = 30 //指定多執行緒數
    val t1 = System.currentTimeMillis
    for ((tag, page) <- tags){
      concurrentCrawler(URL, tag, page, Thread_Num, new ConcurrentHashMap[String, String]())//多執行緒抓取
    }
    val t2 = System.currentTimeMillis
    println(s"抓取數：$sum  重試數：$fail  抓取時間(秒)：" + (t2 - t1) / 1000)
  }
}
