package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;

    private int minVal;

    private int maxVal;

    private double bucketWidth;

    private Estimate estimate;

    private int numTuples;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.minVal = min;
        this.maxVal = max;
        this.bucketWidth = ((maxVal - minVal + 1) * 1.0D) / buckets;
        this.numTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucketIndex = calculateBucketIndex(v);
        ++buckets[bucketIndex];
        ++numTuples;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        Estimate estimate = new Estimate(v);
        switch (op) {
            case EQUALS:
                estimate.setStrategy(new Equals());
                break;
            case GREATER_THAN:
                estimate.setStrategy(new GreaterThan());
                break;
            case GREATER_THAN_OR_EQ:
                estimate.setStrategy(new GreaterThanOrEquals());
                break;
            case LESS_THAN:
                estimate.setStrategy(new LessThan());
                break;
            case LESS_THAN_OR_EQ:
                estimate.setStrategy(new LessThanOrEquals());
                break;
            case NOT_EQUALS:
                estimate.setStrategy(new NotEquals());
                break;
            default:
                throw new IllegalStateException("Not supported op.");
        }
        return estimate.getEstimates();
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "IntHistogram { "
                + "buckets = " + Arrays.toString(buckets)
                + ", minVal = " + minVal
                + ", maxVal = " + maxVal
                + ", bucketWidth = " + bucketWidth
                + ", numTuples = " + numTuples
                + " }";
    }

    private int calculateBucketIndex(int v) {
        return (int) Math.floor((v - minVal) / bucketWidth);
    }

    private interface Strategy {
        double getEstimates(int v);
    }

    private class Estimate {
        private Strategy strategy;
        private int v;

        public Estimate(int v) {
            this.v = v;
        }

        public void setStrategy(Strategy strategy) {
            this.strategy = strategy;
        }

        public double getEstimates() {
            return strategy.getEstimates(v);
        }
    }

    private class Equals implements Strategy {
        @Override
        public double getEstimates(int v) {
            if (v > maxVal || v < minVal) {
                return 0.0;
            }
            int bucketIndex = calculateBucketIndex(v);
            return (buckets[bucketIndex] / bucketWidth) / numTuples;
        }
    }

    private class GreaterThan implements Strategy {
        @Override
        public double getEstimates(int v) {
            if (v < minVal) {
                return 1.0;
            }
            if (v > maxVal) {
                return 0.0;
            }

            int bucketIndex = calculateBucketIndex(v);
            double bRight = (bucketIndex + 1) * bucketWidth;
            double bFrac = buckets[bucketIndex] / (numTuples * 1.0);
            double bPart = (bRight - v) / bucketWidth;
            double selectivity = bFrac * bPart;
            for (int i = bucketIndex + 1; i < buckets.length; ++i) {
                selectivity += buckets[i] / (numTuples * 1.0);
            }
            return selectivity;
        }
    }

    private class GreaterThanOrEquals implements Strategy {
        @Override
        public double getEstimates(int v) {
            if (v <= minVal) {
                return 1.0;
            }
            if (v > maxVal) {
                return 0.0;
            }
            double greaterThan = new GreaterThan().getEstimates(v);
            double equal = new Equals().getEstimates(v);
            return greaterThan + equal;
        }
    }

    private class LessThan implements Strategy {
        @Override
        public double getEstimates(int v) {
            if (v < minVal) {
                return 0.0;
            }
            if (v > maxVal) {
                return 1.0;
            }
            return 1.0 - new GreaterThanOrEquals().getEstimates(v);
        }
    }

    private class LessThanOrEquals implements Strategy {
        @Override
        public double getEstimates(int v) {
            if (v < minVal) {
                return 0.0;
            }
            if (v >= maxVal) {
                return 1.0;
            }
            return 1.0 - new GreaterThan().getEstimates(v);
        }
    }

    private class NotEquals implements Strategy {
        @Override
        public double getEstimates(int v) {
            if (v > maxVal || v < minVal) {
                return 1.0;
            }
            return 1.0 - new Equals().getEstimates(v);
        }
    }
}
