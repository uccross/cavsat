package com.beans;

public class Stats {
	private boolean solved;
	private long conflicts;
	private long decisions;
	private long propagations;
	private double time;

	public boolean isSolved() {
		return solved;
	}

	public void setSolved(boolean solved) {
		this.solved = solved;
	}

	public long getConflicts() {
		return conflicts;
	}

	public void setConflicts(long conflicts) {
		this.conflicts = conflicts;
	}

	public long getDecisions() {
		return decisions;
	}

	public void setDecisions(long decisions) {
		this.decisions = decisions;
	}

	public long getPropagations() {
		return propagations;
	}

	public void setPropagations(long propagations) {
		this.propagations = propagations;
	}

	public double getTime() {
		return time;
	}

	public void setTime(double time) {
		this.time = time;
	}
}
