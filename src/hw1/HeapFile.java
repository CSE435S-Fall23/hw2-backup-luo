package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {

	public static final int PAGE_SIZE = 4096;
	private File f;
	private TupleDesc td;
	private int numPage;

	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.f = f;
		this.td = type;
		this.numPage = 1;
	}

	public File getFile() {
		//your code here
		return this.f;
	}

	public TupleDesc getTupleDesc() {
		//your code here
		return this.td;
	}

	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		//byte[] data = new byte[HeapFile.PAGE_SIZE];
		//try {
		//	RandomAccessFile raf = new RandomAccessFile(f, "r");
		//	raf.seek(id * HeapFile.PAGE_SIZE);
		//	raf.read(data);
		//	HeapPage hp = new HeapPage(id, data, this.getId());
		//	raf.close();
		//	return hp;
		//}catch(IOException e) {
		//	e.printStackTrace();
		//}
		//return null;


		byte[] data = new byte[HeapFile.PAGE_SIZE];
		try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
			raf.seek(id * HeapFile.PAGE_SIZE);
			int bytesRead = raf.read(data);
			if (bytesRead != HeapFile.PAGE_SIZE) {
				throw new IOException("Failed to read the entire page");
			}
			return new HeapPage(id, data, this.getId());
		} catch (IOException e) {
			e.printStackTrace(); // You can replace this with better error handling
			// Alternatively, you can throw a custom exception here.
		}
		return null;


	}

	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return f.hashCode();
	}

	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		//your code here
		byte[] data = p.getPageData();
		long pa = p.getId() * PAGE_SIZE;

		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			raf.seek(pa);
			raf.write(data);
			raf.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		//your code here
		for(int i = 0; i < getNumPages(); i++) {
			HeapPage hp = readPage(i);
			try {
				hp.addTuple(t);
				this.writePage(hp);
				return hp;
			}catch(Exception e) {
				e.printStackTrace();
			}
		}

		//if all other pages are full
		try {
			byte[] data = new byte[HeapFile.PAGE_SIZE];
			HeapPage hp = new HeapPage(numPage, data, getId());
			hp.addTuple(t);
			//System.out.println("Actual number of tuples: " + list.size());
			//System.out.println("Occupied" + hp.slotOccupied(0));
			this.writePage(hp);
			numPage++;
			return hp;
		}catch(Exception e) {
			e.printStackTrace();
		}

		return null;
	}



//	int i = 0;
//	HeapPage hp = null;
//	if (i == this.getNumPages()) {
//		try {
//			hp = new HeapPage(this.getNumPages(), new byte[PAGE_SIZE], this.getId());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		try {
//			hp.addTuple(t);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//
//	this.writePage(hp);
//	return hp;


//	for(int j = 0; j < hp.getNumSlots(); j++) {
//		if(hp.slotOccupied(j)) {
//			try {
//				hp.addTuple(t);
//				this.writePage(hp);
//				return hp;
//			}catch(Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}



	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		try {
			int i = t.getPid();
			HeapPage hp = this.readPage(i);
			hp.deleteTuple(t);
			this.writePage(hp);
		}catch(Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> list = new ArrayList<Tuple>();
		for(int i=0; i < this.getNumPages(); i++) {
			HeapPage hp = readPage(i);
			Iterator<Tuple> iterator = hp.iterator();
			while(iterator.hasNext()) {
				list.add(iterator.next());
			}
		}

		//System.out.println("Actual number of tuples: " + list.size());
		return list;

	}



	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		return numPage;
	}
}
