package loadPhoneNum;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * this is used for multiThread vertex loading 
 * @author	ziploe
 * @E-mail	jmernio@gmail.com
 * @date	2016.10.16
 */
public class ReadThread extends Thread{
	private static Logger logger = Logger.getLogger(ReadThread.class);
	
	int _M=1000000;
	String dataPath;
	LoadFactory lf;
	ArrayBlockingQueue<String> orgQueue;
	
	public ReadThread(ArrayBlockingQueue<String> orgQueue,LoadFactory lf){
		this.orgQueue=orgQueue;
		this.lf=lf;
		PropertyConfigurator.configure(lf.logPath);
		this.dataPath=lf.dataPath;
	}
	
	
	
	BufferedReader bf=null;
	String tmpLine;
	int num=0;
	int dataSize;
	@Override
	public void run(){
		logger.info("read data thread started!");
		try {
			bf=new BufferedReader(new InputStreamReader(new FileInputStream(dataPath)));
			logger.info("reading data file: "+dataPath);
			while((tmpLine=bf.readLine())!=null){
				dataSize++;
				orgQueue.put(tmpLine);
				if(num++%_M==0){
					logger.info(num+" pieces of data has been read to queue");
				}
			}	
			logger.info("total: "+num+" pieces of data read to queue");
			logger.info("data size: "+num);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}finally{
			lf.readEndsFlag=true;
			try {
				bf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				bf=null;
			}
		}		
	}		

}
