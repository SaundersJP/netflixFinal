package overlapJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntIntWritable implements WritableComparable<IntIntWritable> {

  Integer left;
  Integer right;

  /**
   * Empty constructor - required for serialization.
   */ 
  public IntIntWritable() {

  }

  /**
   * Constructor with two Integer objects provided as input.
   */ 
  public IntIntWritable(Integer left, Integer right) {
    this.left = left;
    this.right = right;
  }

  /**
   * Serializes the fields of this object to out.
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(left);
    out.writeInt(right);
  }

  /**
   * Deserializes the fields of this object from in.
   */
  public void readFields(DataInput in) throws IOException {
    left = in.readInt();
    right = in.readInt();
  }

  /**
   * Compares this object to another IntIntWritable object by
   * comparing the left Integers first. If the left Integers are equal,
   * then the right Integers are compared.
   */
  public int compareTo(IntIntWritable other) {
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
  
  public Integer getRight() {
	  return right;
  }
  
  public void  setLeft(Integer left) {
	  this.left = left;
  }
  
  public void setRight(Integer right) {
	  this.right = right;
  }

  /**
   * A custom method that returns the two Integers in the 
   * IntIntWritable object inside parentheses and separated by
   * a comma. For example: "(left,right)".
   */
  public String toString() {
    return "(" + left + "," + right + ")";
  }

  /**
   * The equals method compares two IntIntWritable objects for 
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
    IntIntWritable other = (IntIntWritable) obj;
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
   * The hashCode method generates a hash code for a IntIntWritable 
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
