package index;

import java.io.File;
import java.io.IOException;
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


public class CIndex {

	public static void main(String[] args) {
		String indexPath;		
		if(args.length!=3){
			System.out.println("usage:indexPath ¿Õ¸ñ key ¿Õ¸ñ value");
			System.exit(0);
		}
		
		
		IndexWriter writer = null;
		try {

			indexPath=args[0].trim();
			Directory directory = FSDirectory.open(new File(indexPath));
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LATEST, null);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			iwc.setRAMBufferSizeMB(10240);
			writer =new IndexWriter(directory,iwc );

			Document doc;
			StringField key=new StringField("key", "null", Store.NO);
			StoredField value=new StoredField("value", "null");
			key.setStringValue(args[0].trim());
			value.setStringValue(args[1].trim());
			doc=new Document();
			doc.add(key);
			doc.add(value);
			System.out.println("doc added key:"+args[0].trim()+" value: "+args[1].trim());
			writer.addDocument(doc);

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
