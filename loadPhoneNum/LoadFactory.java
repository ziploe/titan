package loadPhoneNum;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * this is used for multiThread vertex loading
 * @author	ziploe
 * @E-mail	jmernio@gmail.com
 * @date	2016.10.16
 */
public class LoadFactory {
	int _M=1000000;
	ArrayBlockingQueue<String> orgQueue;

	AtomicLong phoneNumCounter;
	boolean readEndsFlag=false;
	Properties conf;
	
	
	String logPath;
	String dataPath;
	String metaDataDir;
	String titanConfPath;
	int titanThreadNum;
	
	public LoadFactory(Properties conf){
		this.orgQueue= new ArrayBlockingQueue<>(_M);

		this.phoneNumCounter=new AtomicLong();
		this.conf=conf;
	
	}
	
	public void init(){
		logPath=conf.getProperty("logPath");
		dataPath=conf.getProperty("dataPath");
		metaDataDir=conf.getProperty("metaDataDir");
		titanConfPath=conf.getProperty("titanConfPath");
		titanThreadNum=Integer.parseInt(conf.getProperty("titanThreadNum"));
		
		ReadThread readThread=new ReadThread(orgQueue, this);
		readThread.start();
		
		for(int i=0;i<titanThreadNum;i++){
			LoadThread loadThread=new LoadThread(orgQueue, this);
			loadThread.setName("titanWorker"+i);
			loadThread.start();
		}
		
	}
	
	
	public static void main(String[] args) {
		if(args.length!=1){
			System.out.println("where is the conf file");
			System.exit(1);
		}
		Properties config= new Properties();
		try {
			InputStreamReader isr=null;	
			isr=new InputStreamReader(new FileInputStream(args[0].trim()));
			config.load(isr);
			isr.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		LoadFactory lf=new LoadFactory(config);
		lf.init();
	}
}
