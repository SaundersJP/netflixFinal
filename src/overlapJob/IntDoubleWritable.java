package overlapJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntDoubleWritable implements WritableComparable<IntDoubleWritable> {

  Integer left;
  Double right;

  /**
   * Empty constructor - required for serialization.
   */ 
  public IntDoubleWritable() {

  }

  /**
   * Constructor with two String objects provided as input.
   */ 
  public IntDoubleWritable(Integer left, Double right) {
    this.left = left;
    this.right = right;
  }

  /**
   * Serializes the fields of this object to out.
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(left);
    out.writeDouble(right);
  }

  /**
   * Deserializes the fields of this object from in.
   */
  public void readFields(DataInput in) throws IOException {
    left = in.readInt();
    right = in.readDouble();
  }

  /**
   * Compares this object to another IntDoubleWritable object by
   * comparing the left strings first. If the left strings are equal,
   * then the right strings are compared.
   */
  public int compareTo(IntDoubleWritable other) {
    int ret = left.compareTo(other.left);
    if (ret == 0) {
      return right.compareTo(other.right);
    }
    return ret;
  }

  /* getters and setters for the two objects in the pair */
  public Integer getLeft() {
	  return left;
  }
  
  public Double getRight() {
	  return right;
  }
  
  public void  setLeft(Integer left) {
	  this.left = left;
  }
  
  public void setRight(Double right) {
	  this.right = right;
  }

  /**
   * A custom method that returns the two strings in the 
   * IntDoubleWritable object inside parentheses and separated by
   * a comma. For example: "(left,right)".
   */
  public String toString() {
    return "(" + left + "," + right + ")";
  }

  /**
   * The equals method compares two IntDoubleWritable objects for 
   * equality. The equals and hashCode methods have been automatically
   * generated by Eclipse by right-clicking on an empty line, selecting
   * Source, and then selecting the Generate hashCode() and equals()
   * option. 
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    IntDoubleWritable other = (IntDoubleWritable) obj;
    if (left == null) {
      if (other.left != null)
        return false;
    } else if (!left.equals(other.left))
      return false;
    if (right == null) {
      if (other.right != null)
        return false;
    } else if (!right.equals(other.right))
      return false;
    return true;
  }

  /**
   * The hashCode method generates a hash code for a IntDoubleWritable 
   * object. The equals and hashCode methods have been automatically
   * generated by Eclipse by right-clicking on an empty line, selecting
   * Source, and then selecting the Generate hashCode() and equals()
   * option. 
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((left == null) ? 0 : left.hashCode());
    result = prime * result + ((right == null) ? 0 : right.hashCode());
    return result;
  }
}