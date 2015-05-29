import java.io.File;
import java.io.PrintWriter;


public class Utils {
	public static String nextBallot = "NEXT_BALLOT";
	public static String initiator = "INITIATOR";
	public static String prepare = "PREPARE";
	public static String promise = "PROMISE";
	public static String accept = "ACCEPT";
	public static String accepted = "ACCEPTED";
	public static String decide = "DECIDE";
	public static String acceptedRound = "ACCEPTEDROUND";
	public static String va = "VA";
	
	public static String QUEUE[] = {"GROUP1", "GROUP2", "GROUP3"};
	public static String request = "REQUEST";
	
	static void clearFiles() throws Exception{
		for (int i=1;i<=3;i++){
			String fileNames[] = {i + ".in", i + "accepted.in", i + "current.in", i + "promise.in"};
			for(int j=0;j<fileNames.length;j++){
				PrintWriter out = new PrintWriter(new File(fileNames[j]));
				out.println(0);
				out.close();
			}
		}
		System.exit(0);
	}
	
}
