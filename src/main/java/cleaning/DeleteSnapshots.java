package cleaning;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import common.MongoDAOUtil;
import common.RangeBean;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rchirinos on 4/21/16.
 */
public class DeleteSnapshots {
    public static final int nThreads = 20;
    public static final String DATE_STRING = "01/01/2016";
    public static final String THING_SNAPSHOTS = "path_thingSnapshots";
    public static final String THING_SNAPSHOTSID = "path_thingSnapshotIds";
    public static final int numTransaction = 30;

    public static void main(String[] args) {
        System.out.println("Starting at "+new Date());
        final long startTime = System.currentTimeMillis();

        Boolean mongoInitialized = initMongo();
        if(mongoInitialized){
            try{
                readFile();
                //executingDataCleaning();
            }catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        final long endTime = System.currentTimeMillis();
        final long total = endTime-startTime;
        System.out.println("TIME: "+total);
        System.out.println("Finished at: "+new Date());
    }

    private static void executingDataCleaning() throws Exception
    {
        DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
        Date date = formatter.parse(DATE_STRING);
        Long timestampDate = date.getTime();

        List<List<Long>> global = new ArrayList<>();

        System.out.println(date);
        System.out.println(timestampDate);
        BasicDBObject query = new BasicDBObject("blinks",
                new BasicDBObject("$elemMatch",
                        new BasicDBObject("time",
                                new BasicDBObject("$lt", timestampDate))));

        System.out.println(query);
        DBCursor cursor = MongoDAOUtil.getInstance().getCollection(THING_SNAPSHOTSID).find(query);
        while( cursor.hasNext() )
        {
            List<Long> res = new ArrayList<>();
            DBObject o = cursor.next();
            BasicDBList blinks = (BasicDBList) o.get("blinks");
            if(blinks!=null)
            {
                for (Object obj: blinks){
                    BasicDBObject basicDBObject = (BasicDBObject) obj;
                    if(((Long)basicDBObject.get("time")).compareTo(timestampDate)==-1){
                        res.add((Long)basicDBObject.get("blink_id"));
                    }
                }
            }
            global.add(res);
        }
        if(global!=null && global.size()>0){
            DeleteSnapshots data = new DeleteSnapshots();
            data.removeSnapshotsOne(global);
        }
    }

    private void removeSnapshotsOne(List<List<Long>> global){
        List<RangeBean> ranges = getRanges(global);
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        for(RangeBean range : ranges){
            List<List<Long>> subList = global.subList(range.getIni(),range.getEnd());
            if(subList!=null) {
                //RunnableDeleteSnapshot runnable = new RunnableDeleteSnapshot(subList);
                //executor.execute(runnable);
            }
        }
    }

    private static List<RangeBean> getRanges(List<List<Long>> global)
    {
        List<RangeBean> ranges = new ArrayList<>();
        if( global.size() < numTransaction){
            int count  = global.size()/numTransaction;
            int ini = 0;
            int end = 0;
            boolean rest = false;
            for (int i = 1; i <= count ; i++) {
                RangeBean range = new RangeBean(ini,end);
                ranges.add(range);
                ini = ini+numTransaction;
                end = end+numTransaction;
                if(end>global.size()){
                    end = (global.size()-(end-numTransaction))+(end-numTransaction);
                    rest = true;
                }
            }
            if(rest)
            {
                RangeBean range = new RangeBean(ini,end);
                ranges.add(range);
            }
        }else{
            RangeBean range = new RangeBean(1,global.size());
            ranges.add(range);
        }

        return ranges;
    }
    private static void readFile() throws Exception
    {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("/home/rchirinos/Documents/ini_ruth/Documents/Mojix/QueriesMongoNLevel/output.txt"));

            List<String> stringDelete = new ArrayList<>();
            List<String> stringUpdate = new ArrayList<>();
            String line = br.readLine();
            while (line != null) {
                if(line.contains("D-")){
                    line = line.replace("D-","");
                    stringDelete.add(line);
                }else if(line.contains("U-"))
                {
                    line = line.replace("U-","");
                    stringUpdate.add(line);
                }
                line = br.readLine();
            }
            DeleteSnapshots data = new DeleteSnapshots();
            data.removeSnapshots(stringDelete);
            data.updateSnapshots(stringUpdate);

        } finally {
            if(br!=null)
            {
                br.close();
            }
        }
    }

    public void removeSnapshots(List<String> stringDelete) {
        //Divide in ranges
        List<RangeBean> ranges = getRangesString(stringDelete);
        if(ranges!=null){
            ExecutorService executor = Executors.newFixedThreadPool(nThreads);
            for(RangeBean range : ranges)
            {
                List<String> subList = stringDelete.subList(range.getIni(),range.getEnd());
                List<List<ObjectId>> longValues = new ArrayList<>();
                for(String data : subList){
                    String[] split = data.split(",");
                    List<ObjectId> lstObjectData = new ArrayList<>();
                    for (int i = 0; i < split.length; i++) {
                        ObjectId id = new ObjectId(split[i]);
                        lstObjectData.add(id);
                    }
                    longValues.add(lstObjectData);
                }
                System.out.println(longValues);

                if(longValues!=null) {
                    RunnableDeleteSnapshot runnable = new RunnableDeleteSnapshot(longValues);
                    executor.execute(runnable);
                }
            }
        }
    }

    public void updateSnapshots(List<String> stringUpdate) {
        //Divide in ranges
        List<RangeBean> ranges = getRangesString(stringUpdate);
        if(ranges!=null){
            ExecutorService executor = Executors.newFixedThreadPool(nThreads);
            for(RangeBean range : ranges)
            {
                List<String> subList = stringUpdate.subList(range.getIni(),range.getEnd());
                if(subList!=null && subList.size()>0)
                {
                    RunnableUpdateSnapshot runnable = new RunnableUpdateSnapshot(subList);
                    executor.execute(runnable);
                }
            }
        }
    }

    public static List<RangeBean> getRangesString(List<String> lst){
        List<RangeBean> ranges = new ArrayList<>();
        if(lst.size()>=numTransaction){
            int count  = lst.size()/numTransaction;
            int ini = 0;
            int end = 0;
            if(count==0){
                count = 1;
                end = lst.size();
            }else{
                end = numTransaction;
            }

            boolean rest = false;
            for (int i = 1; i <= count ; i++) {
                RangeBean range = new RangeBean(ini,end);
                ranges.add(range);
                ini = ini+numTransaction;
                end = end+numTransaction;
                if(end>lst.size()){
                    end = (lst.size()-(end-numTransaction))+(end-numTransaction);
                    rest = true;
                }
            }
            if(rest)
            {
                RangeBean range = new RangeBean(ini,end);
                ranges.add(range);
            }
        }else{
            RangeBean range = new RangeBean(0,lst.size());
            ranges.add(range);
        }
        return ranges;
    }

    private static Boolean initMongo() {
        try {
            MongoDAOUtil.setupMongodb("localhost", 27017, "riot_main", null , null, "admin", "control123!");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public class RunnableDeleteSnapshot implements Runnable {
        List<List<ObjectId>> lstThings;

        public RunnableDeleteSnapshot(List<List<ObjectId>> container) {
            this.lstThings = new ArrayList<>(container.size());
            for(List<ObjectId> lstObjectData: container) {
                List<ObjectId> ids = new ArrayList<>();
                for(ObjectId data :lstObjectData){
                    ids.add(data);
                }
                lstThings.add(ids);
            }
        }

        @Override
        public void run() {
            for (List<ObjectId> ids: lstThings){
                //System.out.println("chico "+ids);
                BasicDBObject query = new BasicDBObject("_id",new BasicDBObject("$in",ids));
                System.out.println(query);
                //MongoDAOUtil.getInstance().getCollection(THING_SNAPSHOTS).remove(query);
            }
        }
    }

    public class RunnableUpdateSnapshot implements Runnable {
        private List<Long> lstThings;
        private long timestampDate =0l;

        public RunnableUpdateSnapshot(List<String> container) {
            this.lstThings = new ArrayList<>(container.size());
            for(String lstObjectData: container) {
                lstThings.add(Long.parseLong(lstObjectData));
            }
            try{
                DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
                Date date = formatter.parse(DATE_STRING);
                timestampDate = date.getTime();
            }catch (Exception e){
                e.printStackTrace();
            }

        }

        @Override
        public void run() {
            //System.out.println("chico "+ids);
            BasicDBObject query = new BasicDBObject("_id",new BasicDBObject("$in",lstThings));
            BasicDBObject condition = new BasicDBObject("$pull",
                    new BasicDBObject("blinks",
                            new BasicDBObject("time",
                                    new BasicDBObject("$lt",timestampDate))));
            System.out.println(query);
            MongoDAOUtil.getInstance().getCollection(THING_SNAPSHOTS).update(query,condition,false,true);
        }
    }
}
