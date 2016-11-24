package index;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class MergeIndex {

	
	public static void main(String[] args) {
		if(args.length!=2){
			System.out.println("[mainIndexPath] + [indicesTobeAddedDir]");
			System.exit(0);
		}
		
		IndexWriter writer = null;
		
		String mainIndexPath;
		String indicesTobeAddedDir;
		
		
		mainIndexPath=args[0].trim();
		indicesTobeAddedDir=args[1].trim();
		System.out.println("mainIndexPath: "+mainIndexPath);
		System.out.println("indicesTobeAddedDir: "+indicesTobeAddedDir);
		
		
		
		File indicesTobeAddedDirFile=new File(indicesTobeAddedDir);
		for(File file :indicesTobeAddedDirFile.listFiles()){
			if(!file.getName().startsWith("index")){
				System.out.println("wrong file: [mainIndexPath] + [indicesTobeAddedDir]");
				System.exit(0);
			}
		}

		
		
		
		long current0 ,current1;

		try {
			Directory mainDirectory = FSDirectory.open(new File(mainIndexPath));
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LATEST, null);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			iwc.setRAMBufferSizeMB(20480);
			writer =new IndexWriter(mainDirectory,iwc );
			
			for(File file :indicesTobeAddedDirFile.listFiles()){
				System.out.println("merging file: "+file.getName());
				current0=System.currentTimeMillis();
				Directory directory2 = FSDirectory.open(file);
				writer.addIndexes(directory2);
				current1=System.currentTimeMillis();
				System.out.println(file.getName()+" done, time cost: "+(current1-current0)+" ms");
			}

			current0=System.currentTimeMillis();
			System.out.println("merge started...");
			writer.forceMerge(3);
			current1=System.currentTimeMillis();
			System.out.println("merge done in :"+(current1-current0)+" ms");
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
}
