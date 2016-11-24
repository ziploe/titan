package loadPhoneNum;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;

/**
 * this is used for multiThread vertex loading 
 * @author	ziploe
 * @E-mail	jmernio@gmail.com
 * @date	2016.10.16
 */
public class LoadThread extends Thread{
	private static Logger logger = Logger.getLogger(LoadThread.class);
	
	LoadFactory lf;
	ArrayBlockingQueue<String> orgQueue;
	AtomicLong phoneNumCounter;	
	TitanVertex tmpV;
	String tmpLine;
	int threadProcessedDataNum=0;
	TitanGraph graph;
	GraphTraversalSource g;
	FileWriter writer=null;
	
	int	batchSize=10000;
	
	String metaDataDir;
	String titanConfPath;
	
	public LoadThread(ArrayBlockingQueue<String> orgQueue,LoadFactory lf){
		this.orgQueue=orgQueue;
		this.lf=lf;
		init();
	}
	
	
	public void init(){
		PropertyConfigurator.configure(lf.logPath);
		this.metaDataDir=lf.metaDataDir;
		this.titanConfPath=lf.titanConfPath;
	}
	
	
	@Override
	public void run(){
		logger.info(this.getName()+" started!");

		try {
			String currentMetaDataPath=metaDataDir+this.getName();
			File file=new File(currentMetaDataPath);
			if(file.exists()){
				logger.error("metaData File already exists! ,you may destory that file!");
				logger.error("metaData File path: "+currentMetaDataPath);
				logger.error("leaving system ...");
				System.exit(1);
			}
			writer=new FileWriter(file,true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		
		long graphStart=System.currentTimeMillis();
		graph=TitanFactory.open(titanConfPath);
		g=graph.traversal();
		long graphEnd=System.currentTimeMillis();
		logger.info("graph created in "+(graphEnd-graphStart)+" ms");

		long start=System.currentTimeMillis();
		long current,startT=start;
		try{
			while(orgQueue.size()==0){
				sleep(500);
			}
			tmpLine=orgQueue.peek();
			if(tmpLine!=null){
				if(ifVertexExist(tmpLine)){
					logger.error("point already exists!: "+tmpLine);
					logger.error("leaving system...");
					System.exit(0);
				}
			}
			
			while(true){
				tmpLine=orgQueue.poll();
				if(null==tmpLine){
					if(lf.readEndsFlag){
						break;
					}
					sleep(5000);
					logger.info("I think I'm running too fast,I will sleep for "+5000+" ms and wait you for a while");
					continue;	
				}			
				try {
					addVertex(tmpLine);
					threadProcessedDataNum++;
				} catch (Exception e) {
					e.printStackTrace();
					logger.info("bad format! :"+tmpLine);	
					continue;
				}
				if(threadProcessedDataNum%batchSize==0){
					graph.tx().commit();
					writer.flush();
					current=System.currentTimeMillis();
					logger.info(this.getName()+": "+threadProcessedDataNum+" number vertex added with last "
							+batchSize+" time costs "+(current-startT)+" ms"+" orgQueue size: "+orgQueue.size());
					startT=System.currentTimeMillis();
				}
			}				

		}catch(Exception e){
			
		}finally{
			current=System.currentTimeMillis();
			logger.info(this.getName()+": threadTotal "+threadProcessedDataNum+" number of vertices had been added "
					+"time costs "+(current-start)/1000+" s");
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			graph.close();
		}
	}


	/**
	 * 灏嗕竴琛屾暟鎹浆鍖栦负鐐�?�互鍙婂畠鐨勫睘鎬�
	 * @param line
	 * @throws Exception
	 */
	void addVertex(String line) throws Exception{
		tmpV=graph.addVertex("phoneNum",line);	
		writer.write(line+"\t"+tmpV.longId()+"\n");	
		if(lf.phoneNumCounter.getAndIncrement()%(50*batchSize)==0){
			logger.info("total: "+lf.phoneNumCounter.get()+" number of vertices had been added");
		}
	}
	
	boolean ifVertexExist(String line){
		long count=g.V().has("phoneNum",line).count().next();
		return count>0;
	}

}
