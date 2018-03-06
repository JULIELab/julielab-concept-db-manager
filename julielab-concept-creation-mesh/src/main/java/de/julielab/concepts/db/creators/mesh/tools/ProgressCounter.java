package de.julielab.concepts.db.creators.mesh.tools;

import org.slf4j.Logger;

/**
 * This class provides a textual progress counter. 
 * 
 * Each time the counter is increased and reached the next step (e.g. every 5% of progress) a message will be printed:
 * " Done with " + cnt + " " + countedObject + " - " + current*step + "%"
 * 
 * @author Philipp Lucas
 */
public class ProgressCounter {
	
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(ProgressCounter.class);
	
    private int size; // total number of objects to process
    private int cnt; // counts the number of objects processed so far
    private int step; 
    private int current; // current process, describes the process in terms of %-step as in step
    private String countedObject; // name of things counted
    //PrintStream outStream; // print stream to print to
    
    /**
     * @param size total number of steps till task is done, or 0 if it is unknown.
     * @param step 'Every step-th %' (size != 0) or 'every step-th element' (size == 0) of the process a msg will be written.
     * @param countedObject Name of the object that is processed.
     */
    public ProgressCounter(int size, int step, String countedObject) {
        this.size = (size==0?0:size-1);
        this.step = step;
        if(size != 0 && (step < 1 || step >= 100)) {
            step = 1;
        }
        this.countedObject = countedObject;
        //this.outStream = System.out;
        reset();
    }
    
//    /**
//     * @param size total number of steps till task is done.
//     * @param step Every step-th % of the process a msg will be written.
//     * @param countedObject Name of the object that is processed.
//     * @param outStream PrintStream to write msg to.
//     */
//    public ProgressCounter(int size, int step, String countedObject, PrintStream outStream) {
//        this.size = (size==0?0:size-1);
//        this.step = step;
//        if(step < 1 || step >= 100) {
//            step = 1;
//        }
//        this.countedObject = countedObject;
//        this.outStream = outStream;
//        reset();
//    }
       
    /**
     * Resets the progress.
     */
    final public void reset() {
        this.cnt = 0;
        this.current = 0;
    }
    
    /**
     * Increment progress by 1. If next "step" is reached a msg will be written.
     */
    public void inc() {
        cnt++;
        pushMsg();
    }
    
    /**
     * Increment progress by by. If next "step" is reached a msg will be written.
     * @param by Number how much progress should be inreased.
     */
    public void inc(int by) {
        cnt += by;
        pushMsg();
    }
    
    /**
     * 
     * @return Return the number of objects processed so far
     */
    public int getCount() {
    	return cnt;
    }

    private void pushMsg() {
        try {
            if (size == 0) {
                if (cnt % step == 0) {
                    logger.info(" Done with " + cnt + " " + countedObject + "s");
                    //outStream.flush();
                }
            } else if (current != cnt * (100 / step) / size) {
                current = cnt * (100 / step) / size;
                logger.info(" Done with " + cnt + " " + countedObject + "s - " + current * step + "%");
                //outStream.flush();
            }
        } catch (ArithmeticException e) {
            logger.error(e.getMessage());
        }
    }

    public void startMsg() {
        if (size == 0) {
            logger.info("Start processing ...");
        } else {
            logger.info("There are " + size + " " + countedObject + "s. Start processing ... ");
        }
        //outStream.flush();
    }
    
    public void finishMsg() {
        if (size == 0) {
            logger.info("... Finished processing all " + cnt + " " + countedObject +"s.");
        } else {
            logger.info("... There were " + size + " " + countedObject + "s. Finished them  all. ");
        }
        //outStream.flush();
    }
}
