package index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;


public class IndexOnlyPhoneNum {
	private static Logger logger = Logger.getLogger(IndexOnlyPhoneNum.class);
	public static void main(String[] args) {
		
		int callSize=1000000;
		String logConfPath;
		String dataPath=null;
		String indexPath=null;
		
		if(args.length!=1){
			System.out.println("where is the properties file"+"\n"
					+ "properties file must contain dataPath,indexPath,logConfPath ");
			System.exit(0);		
		}
		

		try {
			Properties properties= new Properties();
			InputStreamReader isr=null;	
			isr=new InputStreamReader(new FileInputStream(args[0].trim()));
			properties.load(isr);
			isr.close();
			dataPath=properties.getProperty("dataPath");
			indexPath=properties.getProperty("indexPath");
			logConfPath=properties.getProperty("logConfPath");

			PropertyConfigurator.configure(logConfPath);
			
			logger.info("logConfPath: "+logConfPath);	
			logger.info("dataPath: "+dataPath);
			logger.info("indexPath: "+indexPath);		
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		logger.info("initial succeed! index is starting! ");
		IndexWriter writer = null;
		try {
			Directory directory = FSDirectory.open(new File(indexPath));
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LATEST, null);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			iwc.setRAMBufferSizeMB(20480);

			writer =new IndexWriter(directory,iwc );

			Document doc;
			InputStreamReader reader = new InputStreamReader( new FileInputStream(dataPath));
			BufferedReader br = new BufferedReader(reader); 
			String line;  	
			int num=0;
			StringField key=new StringField("phoneNum", "null", Store.NO);
			String phoneNum;
			
			
			long start=System.currentTimeMillis();
			long current,startT=start;
			logger.info("index started!");
			while ((line=br.readLine())!=null) { 
				phoneNum=line.trim();
				key.setStringValue(phoneNum);
				doc=new Document();
				doc.add(key);

				writer.addDocument(doc);
				if(++num%callSize==0){
					
					current=System.currentTimeMillis();
					logger.info(num+"number phoneNum indexed with last "
							+callSize+" time costs"+(current-startT)+"ms");
					startT=System.currentTimeMillis();
				}

			}
			current=System.currentTimeMillis();
			logger.info("total: "+num+" pieces of datas has been indexed, total time costs is: "+
					(current-start)/1000+" s");
			
			startT=System.currentTimeMillis();
			writer.forceMerge(2);
			current=System.currentTimeMillis();
			logger.info("merge ends in: "+(current-startT)+" ms");

			br.close();

		} catch (IOException e) {	
			e.printStackTrace();
		}finally {
			try {
				writer.close();
			} catch (CorruptIndexException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}		
	
}
