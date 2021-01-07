import redis.clients.jedis.*;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class RedisTest {
    public static boolean stop = false;
    static ShardedJedisPool pool;
    static String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public static void setPool(String ip, String port, String pass) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(50);
        config.setMaxWaitMillis(3000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        JedisShardInfo jedisShardInfo1 = new JedisShardInfo(ip, Integer.parseInt(port));
        jedisShardInfo1.setPassword(pass);
        List<JedisShardInfo> list = new LinkedList<JedisShardInfo>();
        list.add(jedisShardInfo1);
        pool = new ShardedJedisPool(config, list);
    }

    public static void writeData(long setCount) {
        ShardedJedis jedis = pool.getResource();
        long startTime = System.currentTimeMillis();
        for (long i = 0; i < setCount; i++) {
            jedis.set(String.valueOf(i), String.valueOf(System.currentTimeMillis()));
            if (i % 100000 == 0) {
                System.out.println(i);
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("run time : " + (endTime - startTime));
        System.out.println("write data QPS : " + setCount / (endTime - startTime) * 1000.0);
    }


    public static String getRandomString(int length) {
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    static class MyCallable implements Callable<Integer> {
        public Integer call() throws InterruptedException {
            String suffix = getRandomString(10);
            ShardedJedis jedis = pool.getResource();
            int sum = 0;
            for (int i = 0; !stop; i++) {
                jedis.set(Thread.currentThread().getName() + String.valueOf(i) + suffix, String.valueOf(System.currentTimeMillis()));
                sum++;
            }
            return sum;
        }

    }

    private static double getPercentile(Vector<Long> array, double percentile) {
        Collections.sort(array);
        double x = (array.size() - 1) * percentile;
        int i = (int) x;
        double j = x - i;
        return (1 - j) * array.elementAt(i) + j * array.elementAt(i + 1);
    }


    public static void writeRandomData(int threadCount, long writeTime) throws InterruptedException {

        Callable<Integer> myCallable = new MyCallable();
        Vector<FutureTask<Integer>> futureTasks = new Vector<FutureTask<Integer>>();
        Vector<Thread> threads = new Vector<Thread>();

        for (int i = 0; i < threadCount; i++) {
            FutureTask<Integer> ft = new FutureTask<Integer>(myCallable);
            futureTasks.add(ft);
            threads.add(new Thread(ft));
            threads.elementAt(i).start();
        }

        Thread.sleep(writeTime * 1000);
        stop = true;
        long totalWrite = 0;
        try {
            for (int i = 0; i < threadCount; i++) {
                int sum = futureTasks.elementAt(i).get();
                System.out.println("sum = " + sum);
                totalWrite += sum;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("run time : " + writeTime);
        System.out.println("totalWrite : " + totalWrite);
        System.out.println("write random data QPS : " + totalWrite / writeTime * 1.0);

    }

    public static void getData(long getCount) throws InterruptedException {
        ShardedJedis jedis = pool.getResource();
        long totalTime = 0;
        long readed = 0;
        Vector<Long> array = new Vector<Long>();
        while (readed < getCount) {
            String result = jedis.get(String.valueOf(readed));
            if (result == null) {
                Thread.sleep(0);
                continue;
            }
            long getTime = System.currentTimeMillis();
            totalTime += getTime - Long.parseLong(result);
            array.add(getTime - Long.parseLong(result));
            readed++;
            if (readed % 100000 == 0) {
                System.out.println(readed + "\t" + (getTime - Long.parseLong(result)));
            }
        }
        System.out.println("total time : " + totalTime);
        System.out.println("average time : " + totalTime / (double) (readed));
        System.out.println("P99 : " + getPercentile(array, 0.99));
        System.out.println("P95 : " + getPercentile(array, 0.95));
        System.out.println("P50 : " + getPercentile(array, 0.50));
    }

    public static void main(String[] args) throws InterruptedException {
        String ip = args[0];
        String port = args[1];
        String passwd = args[2];
        String setCount = args[3];
        String getOrSet = args[4];
        //String WriteTime = args[5];

        setPool(ip, port, passwd);

        if (getOrSet.trim().equals("set")) {
            System.out.println("set data to redis, data size is " + Long.parseLong(setCount));
            writeData(Long.parseLong(setCount));
        } else if (getOrSet.trim().equals("get")) {
            System.out.println("get data from redis, data size is " + Long.parseLong(setCount));
            getData(Long.parseLong(setCount));
        } else {
            System.out.println("just write random data, data size is " + Long.parseLong(setCount));
            //writeRandomData(Integer.parseInt(setCount), Long.parseLong(WriteTime));
        }
    }
}