package edu.nyu.bigdataproject.helper;

/**
 * Created by mindaf93 on 4/11/17.
 */
public class ResultType {
    public final boolean valid;
    public final long date;
    ResultType(boolean valid, long date) {
        this.valid = valid;
        this.date = date;
    }
}