package spark

import java.io._

class JavaSerializationStream(out: OutputStream) extends SerializationStream {
 // 使用 java 的object output Stream
  val objOut = new ObjectOutputStream(out)
  def writeObject[T](t: T) { objOut.writeObject(t) }
  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

class JavaDeserializationStream(in: InputStream) extends DeserializationStream {
 // 使用 java 的object input Stream
  val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) =
      Class.forName(desc.getName, false, Thread.currentThread.getContextClassLoader)
  }

  def readObject[T](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}

class JavaSerializerInstance extends SerializerInstance {
// 把对象变成序列化bytes
  def serialize[T](t: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = outputStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val in = inputStream(bis)
    // 从bytes 中反序列化
    in.readObject().asInstanceOf[T]
  }

  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    return ois.readObject.asInstanceOf[T]
  }
 // 返回给用户 可以序列化的流
  def outputStream(s: OutputStream): SerializationStream = {
     new JavaSerializationStream(s)
   }

   def inputStream(s: InputStream): DeserializationStream = {
     new JavaDeserializationStream(s)
   }
 }

class JavaSerializer extends Serializer {
  def newInstance(): SerializerInstance = new JavaSerializerInstance
}
