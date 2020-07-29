package spark

import java.io._

import scala.collection.mutable.Map
import scala.collection.generic.Growable

class Accumulable[T,R] (
    @transient initialValue: T,
    param: AccumulableParam[T,R])
  extends Serializable {
  
  val id = Accumulators.newId
  // master 上的初始 value
  @transient
  private var value_ = initialValue // Current value on master
  // 使用初始化变量创建一个param 对象，准备传给workers
  val zero = param.zero(initialValue)  // Zero value to be passed to workers
  var deserialized = false

  // 注册一下自己，注册在 origin里面，key是id value 是自己
  Accumulators.register(this, true)

  /**
   * add more data to this accumulator / accumulable
   * @param term the data to add
   */
   // 将worker 上的数据添加到value 里
  def += (term: R) { value_ = param.addAccumulator(value_, term) }

  /**
   * merge two accumulable objects together
   * 
   * Normally, a user will not want to use this version, but will instead call `+=`.
   * @param term the other Accumulable that will get merged with this
   */
  def ++= (term: T) { value_ = param.addInPlace(value_, term)}
  // 不是分序列化出来的
  def value = {
    if (!deserialized) value_
    else throw new UnsupportedOperationException("Can't use read value in task")
  }

  /**
   * Get the current value of this accumulator from within a task.
   *
   * This is NOT the global value of the accumulator.  To get the global value after a
   * completed operation on the dataset, call `value`.
   *
   * The typical use of this method is to directly mutate the local value, eg., to add
   * an element to a Set.
   */
  def localValue = value_

  // 不是反序列化的，设置值
  def value_= (t: T) {
    if (!deserialized) value_ = t
    else throw new UnsupportedOperationException("Can't use value_= in task")
  }
 
  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject
    value_ = zero
    deserialized = true
    Accumulators.register(this, false)
  }

  override def toString = value_.toString
}

class Accumulator[T](
    @transient initialValue: T,
    param: AccumulatorParam[T]) extends Accumulable[T,T](initialValue, param)

/**
 * A simpler version of [[spark.AccumulableParam]] where the only datatype you can add in is the same type
 * as the accumulated value
 * @tparam T
 */
trait AccumulatorParam[T] extends AccumulableParam[T,T] {
  def addAccumulator(t1: T, t2: T) : T = {
    addInPlace(t1, t2)
  }
}

/**
 * A datatype that can be accumulated, ie. has a commutative & associative +.
 *
 * You must define how to add data, and how to merge two of these together.  For some datatypes, these might be
 * the same operation (eg., a counter).  In that case, you might want to use [[spark.AccumulatorParam]].  They won't
 * always be the same, though -- eg., imagine you are accumulating a set.  You will add items to the set, and you
 * will union two sets together.
 *
 * @tparam R the full accumulated data
 * @tparam T partial data that can be added in
 */
trait AccumulableParam[R,T] extends Serializable {
  /**
   * Add additional data to the accumulator value.
   * @param t1 the current value of the accumulator
   * @param t2 the data to be added to the accumulator
   * @return the new value of the accumulator
   */
  def addAccumulator(t1: R, t2: T) : R

  /**
   * merge two accumulated values together
   * @param t1 one set of accumulated data
   * @param t2 another set of accumulated data
   * @return both data sets merged together
   */
  def addInPlace(t1: R, t2: R): R

  def zero(initialValue: R): R
}

class GrowableAccumulableParam[R <% Growable[T] with TraversableOnce[T] with Serializable, T]
extends AccumulableParam[R,T] {
  def addAccumulator(growable: R, elem: T) : R = {
    growable += elem
    growable
  }

  def addInPlace(t1: R, t2: R) : R = {
    t1 ++= t2
    t1
  }

  def zero(initialValue: R): R = {
    // We need to clone initialValue, but it's hard to specify that R should also be Cloneable.
    // Instead we'll serialize it to a buffer and load it back.
    val ser = (new spark.JavaSerializer).newInstance
    val copy = ser.deserialize[R](ser.serialize(initialValue))
    copy.clear()   // In case it contained stuff
    copy
  }
}

// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private object Accumulators {
  // TODO: Use soft references? => need to make readObject work properly then
  val originals = Map[Long, Accumulable[_,_]]()
  val localAccums = Map[Thread, Map[Long, Accumulable[_,_]]]()
  var lastId: Long = 0

  // 新的id
  def newId: Long = synchronized {
    lastId += 1
    return lastId
  }

  def register(a: Accumulable[_,_], original: Boolean): Unit = synchronized {
  // 不用original 的话，就为当前线程新创建一个map，使用id 注册自己
    if (original) {
      originals(a.id) = a
    } else {
      val accums = localAccums.getOrElseUpdate(Thread.currentThread, Map())
      accums(a.id) = a
    }
  }

  // Clear the local (non-original) accumulators for the current thread
  def clear: Unit = synchronized {
    // 清除当前线程的acc们
    localAccums.remove(Thread.currentThread)
  }

  // Get the values of the local accumulators for the current thread (by ID)
  // 获得所有local accumlators 的值
  def values: Map[Long, Any] = synchronized {
    val ret = Map[Long, Any]()
    for ((id, accum) <- localAccums.getOrElse(Thread.currentThread, Map())) {
      ret(id) = accum.localValue
    }
    return ret
  }

  // Add values to the original accumulators with some given IDs
  def add(values: Map[Long, Any]): Unit = synchronized {
    // 把 values 的每一项添加在相应的 acc 后面
    for ((id, value) <- values) {
      if (originals.contains(id)) {
        originals(id).asInstanceOf[Accumulable[Any, Any]] ++= value
      }
    }
  }
}
