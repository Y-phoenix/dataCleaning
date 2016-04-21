package cleaning;

import com.mongodb.BasicDBObject;
import common.MongoDAOUtil;
import common.RangeBean;

import java.io.BufferedReader;
import java.io.FileReader;
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
    public static final String THING_SNAPSHOTS = "path_thingSnapshots";
    public static final int numTransaction = 30;

    public static void main(String[] args) {
        System.out.println("Starting at "+new Date());
        final long startTime = System.currentTimeMillis();

        Boolean mongoInitialized = initMongo();
        if(mongoInitialized){
            try{
                readFile();
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

    private static Boolean initMongo() {
        try {
            MongoDAOUtil.setupMongodb("localhost", 27017, "riot_main", null , null, "admin", "control123!");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private static void readFile() throws Exception
    {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("/home/rchirinos/Documents/testData.txt"));

            List<String> stringDelete = new ArrayList<>();
            String line = br.readLine();
            while (line != null) {
                if(line.contains("D-")||line.contains("U-")){
                    line = line.replace("D-","");
                    line = line.replace("U-","");
                    stringDelete.add(line);
                }
                line = br.readLine();
            }
            DeleteSnapshots data = new DeleteSnapshots();
            data.removeSnapshots(stringDelete);

        } finally {
            if(br!=null)
            {
                br.close();
            }
        }
    }

    public void removeSnapshots(List<String> stringDelete) {
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        int count  = stringDelete.size()/numTransaction;
        int ini = 1;
        int end = numTransaction;
        boolean rest = false;
        List<RangeBean> ranges = new ArrayList<>();
        for (int i = 1; i <= count ; i++) {
            RangeBean range = new RangeBean(ini,end);
            ranges.add(range);
            ini = ini+numTransaction;
            end = end+numTransaction;
            if(end>stringDelete.size()){
                end = (stringDelete.size()-(end-numTransaction))+(end-numTransaction);
                rest = true;
            }
        }
        if(rest)
        {
            RangeBean range = new RangeBean(ini,end);
            ranges.add(range);
        }

        for(RangeBean range : ranges){
            List<String> subList = stringDelete.subList(range.getIni(),range.getEnd());
            List<List<Long>> longValues = new ArrayList<>();
            for(String data : subList){
                String[] split = data.split(",");
                List<Long> longData = new ArrayList<>();
                for (int i = 0; i < split.length; i++) {
                    Long id = Long.parseLong(split[i]);
                    longData.add(id);
                }
                longValues.add(longData);
            }
            if(longValues!=null) {
                RunnableDeleteSnapshot runnable = new RunnableDeleteSnapshot(longValues);
                executor.execute(runnable);
            }
        }
    }

    public class RunnableDeleteSnapshot implements Runnable {
        List<List<Long>> idsLocal;

        public RunnableDeleteSnapshot(List<List<Long>> container) {
            this.idsLocal = new ArrayList<>(container.size());
            for(List<Long> longItems: container) {
                List<Long> ids = new ArrayList<>();
                for(Long data :longItems){
                    ids.add(data);
                }
                idsLocal.add(ids);
            }
        }
        @Override
        public void run() {
            for (List<Long> ids:idsLocal){
                //System.out.println("chico "+ids);
                BasicDBObject query = new BasicDBObject("_id",new BasicDBObject("$in",ids));
                System.out.println(query);
                MongoDAOUtil.getInstance().getCollection(THING_SNAPSHOTS).remove(query);
            }
        }
    }
}
