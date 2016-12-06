package sqlpt

package object hive {
  type Hql = String

  type Translator[T] = T => Hql
}
