import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;


/**
 * <p>Title: TestQueue.java</p>
 * <p>Description: </p>
 *
 * @author T
 * @date Mar 30, 2016
 */
public class TestQueue
{
	public static void main(String[] args) {
		final String[] names =
			{"carol", "alice", "malory", "bob", "alex", "jacobs"};

		final BlockingQueue queue = new PriorityBlockingQueue<>();

		new Thread(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i < names.length; i++) {
					try {
						queue.put(names[i]);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}, "Producer").start();

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					for (int i = 0; i < names.length; i++) {
						System.out.println(queue.take());
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}, "Consumer").start();
	}
}
