package spark

import java.io.File
import java.net.InetAddress

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerList
import org.eclipse.jetty.server.handler.ResourceHandler
import org.eclipse.jetty.util.thread.QueuedThreadPool

/**
 * Exception type thrown by HttpServer when it is in the wrong state for an operation.
 */
class ServerStateException(message: String) extends Exception(message)

/**
 * An HTTP server for static content used to allow worker nodes to access JARs added to SparkContext
 * as well as classes created by the interpreter when the user types in code. This is just a wrapper
 * around a Jetty server.
 */
 // 在 master 上启动了一个 server ，在master上，是为了让worker 可以拿到jar包，
 // 在worker 上，是为了让其他worker 拿shuffle 文件
class HttpServer(resourceBase: File) extends Logging {
  private var server: Server = null
  private var port: Int = -1
    // 启动 jetty
  def start() {
    if (server != null) {
      throw new ServerStateException("Server is already started")
    } else {
      server = new Server(0)
      val threadPool = new QueuedThreadPool
      threadPool.setDaemon(true)
      // 最少8 线程
      threadPool.setMinThreads(System.getProperty("spark.http.minThreads", "8").toInt)
      server.setThreadPool(threadPool)
      // 资源处理器
      val resHandler = new ResourceHandler
      // 资源所在的位置
      resHandler.setResourceBase(resourceBase.getAbsolutePath)
      val handlerList = new HandlerList
      // 默认处理器与资源处理器
      handlerList.setHandlers(Array(resHandler, new DefaultHandler))
      server.setHandler(handlerList)
      // 启动server
      server.start()
      // 获得本地port
      port = server.getConnectors()(0).getLocalPort()
    }
  }

  def stop() {
    if (server == null) {
      throw new ServerStateException("Server is already stopped")
    } else {
      server.stop()
      port = -1
      server = null
    }
  }

  /**
   * Get the URI of this HTTP server (http://host:port)
   */
   // http server的uri
  def uri: String = {
    if (server == null) {
      throw new ServerStateException("Server is not started")
    } else {
      return "http://" + Utils.localIpAddress + ":" + port
    }
  }
}
