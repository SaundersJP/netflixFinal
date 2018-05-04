package overlapJob;

public class Pair<L,R>{
	private final L left;
	private final R right;
	
	public Pair(L left, R right){
		this.left = left;
		this.right = right;
	}
	
	public L getLeft() { return left; }
	public R getRight() { return right; }
	
	@Override
	public int hashCode(){ return left.hashCode() ^ right.hashCode(); }
	
	@Override
	public boolean equals(Object o){
		if (!(o instanceof Pair)) return false;
		Pair pairCheck = (Pair) o;
		return (this.left.equals(pairCheck.getLeft()) && this.right.equals(pairCheck.getRight()));
	}
}