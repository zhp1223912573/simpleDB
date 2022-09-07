package simpledb;

import org.apache.log4j.Logger;

/**
 * @author zhp
 * @date 2022-03-28 14:20
 */
public class myLogger {

    public   static Logger logger = Logger.getLogger(myLogger.class);

    public static void main(String[] args) {
        logger.debug("this is debug message");
        logger.info("this is info message");
        logger.error("this is error message");

    }


}
