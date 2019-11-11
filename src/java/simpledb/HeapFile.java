package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * Read the specified page from disk. Only called by BufferPool when no records in cache
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        //First, we import the buffer pool to get the pages' information
        Database.getBufferPool();
        int totalPages = numPages();

        byte[] page = new byte[BufferPool.getPageSize()];

        //If the pid's page number exceeds the pages in the file return exception
        if (pid.getPageNumber() >= totalPages) {
            throw new IllegalArgumentException("PageId is too big");
        }

        //Try to read the page from the file with fileinputstream and return it in a HeapPage
        // If the file is not found or page number is out of bounds, throw exception
        try {
            FileInputStream fis = new FileInputStream(f);
            fis.skip(pid.getPageNumber() * BufferPool.getPageSize());
            fis.read(page);
            fis.close();
            return new HeapPage((HeapPageId) pid, page);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("file not found");
        } catch (IOException i) {
            throw new IllegalArgumentException("page number out of bounds");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        HeapPage p = (HeapPage) page;

        byte[] data = p.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(p.getId().getPageNumber() * BufferPool.getPageSize());
        rf.write(data);
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double) f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> affectedPages = new ArrayList<>();

        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                //page的insertTuple已经负责修改tuple信息表明其存储在该page上
                page.insertTuple(t);
                page.markDirty(true, tid);
                affectedPages.add(page);
                return affectedPages;
            }
        }
        synchronized (this) {
            BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
            byte[] emptyData = HeapPage.createEmptyPageData();
            bw.write(emptyData);
            bw.close();
        }

        // by virtue of writing these bits to the HeapFile, it is now visible.
        // so some other dude may have obtained a read lock on the empty page
        // we just created---which is ok, we haven't yet added the tuple.
        // we just need to lock the page before we can add the tuple to it.

        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1),
                Permissions.READ_WRITE);
        p.insertTuple(t);
        affectedPages.add(p);
        return affectedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pages = new ArrayList<>();

        HeapPageId pageId = new HeapPageId(getId(), t.getRecordId().getPageId().getPageNumber());
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        pages.add(page);
        return pages;

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * 这个类在实现时有不少疑惑，参考了别人的代码才清楚以下一些点：
     * 1.tableid就是heapfile的id，即通过getId。。但是这个不是从0开始的，按照课程源码推荐，这是文件的哈希码。。
     * 2.PageId是从0开始的。。。(哪里说了，这个可以默认的么，谁知道这个作业的设计是不是从0开始。。。)
     * 3.transactionId哪里来的让我非常困惑，现在知道不用理，反正iterator方法的调用者会提供，应该是以后章节的内容
     * 4.我觉得别人的一个想法挺好，就是存储一个当前正在遍历的页的tuples迭代器的引用，这样一页一页来遍历
     */
    private class HeapFileIterator implements DbFileIterator {

        private int pagePos;

        private Iterator<Tuple> tuplesInPage;

        private TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
            // 不能直接使用HeapFile的readPage方法，而是通过BufferPool来获得page，理由见readPage()方法的Javadoc
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pagePos = 0;
            HeapPageId pid = new HeapPageId(getId(), pagePos);
            //加载第一页的tuples
            tuplesInPage = getTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tuplesInPage == null) {
                //说明已经被关闭
                return false;
            }
            //如果当前页还有tuple未遍历
            if (tuplesInPage.hasNext()) {
                return true;
            }
            //如果遍历完当前页，测试是否还有页未遍历
            //注意要减一，这里与for循环的一般判断逻辑（迭代变量<长度）不同，是因为我们要在接下来代码中将pagePos加1才使用
            //如果不理解，可以自己举一个例子想象运行过程
            if (pagePos < numPages() - 1) {
                pagePos++;
                HeapPageId pid = new HeapPageId(getId(), pagePos);
                tuplesInPage = getTuplesInPage(pid);
                //这时不能直接return ture，有可能返回的新的迭代器是不含有tuple的
                return tuplesInPage.hasNext();
            } else return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("not opened or no tuple remained");
            }
            return tuplesInPage.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            //直接初始化一次。。。。。
            open();
        }

        @Override
        public void close() {
            pagePos = 0;
            tuplesInPage = null;
        }
    }
}

