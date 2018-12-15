package cn.zhubin.sparktest;

import java.io.Serializable;

import scala.math.Ordered;

public class SecondSortKey implements Serializable,Ordered<SecondSortKey>{

	
	private int first;
	private int second;
	
	
	
	public String toString() {
		return "SecondSortKey [first=" + first + ", second=" + second + "]";
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondSortKey other = (SecondSortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
	
	public SecondSortKey(int first, int second) {
		super();
		this.first = first;
		this.second = second;
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	public boolean $greater(SecondSortKey that) {
		if(this.getFirst() > that.getFirst())
			return true;
		else if (this.getSecond() > that.getSecond()) {
			return true;
		}
		return false;
	}

	public boolean $greater$eq(SecondSortKey that) {
		if(this.$greater(that)) {
			return true;
		}else if (this.getFirst()==that.getFirst() && this.getSecond() == that.getSecond()) {
			return true;
		}
		return false;
	}

	public boolean $less(SecondSortKey that) {
		if(this.getFirst() < that.getFirst()) {
			return true;
		}else if(this.getSecond() < that.getSecond()) {
			return true;
		}
		return false;
	}

	public boolean $less$eq(SecondSortKey that) {
		if(this.$less(that)) {
			return true;
		}else if(this.getFirst() == that.getFirst() && this.getSecond() == that.getSecond()) {
			return true;
		}
		return false;
	}

	public int compare(SecondSortKey that) {
		if(this.getFirst() - that.getFirst() != 0) {
			return this.getFirst() - that.getFirst();
		}else {
			return this.getSecond() - that.getSecond();
		}
	}

	public int compareTo(SecondSortKey that) {
		if(this.getFirst() - that.getFirst() != 0) {
			return this.getFirst() - that.getFirst();
		}else {
			return this.getSecond() - that.getSecond();
		}
	}
	
}
