class ThreadsFun {
    public static void main(String[] args) {
        int MAX = 100;

        Thread[] threads = new Thread[MAX];

        for (int i = 0; i < 100; i++) {
            System.out.println("Creating Thread #" + i);
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.currentThread().sleep(100000);
                    } catch (InterruptedException ie) {}
                }
            });

            threads[i].start();

            try {
                Thread.currentThread().sleep(100);
            } catch (InterruptedException ie) {}
        }
    }
}
