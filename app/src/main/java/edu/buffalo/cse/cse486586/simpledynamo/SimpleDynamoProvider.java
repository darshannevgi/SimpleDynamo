package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	int[] portNumbers = {11124,11112,11108,11116,11120};
	private int myPort;

	private int getIndex(int port)
	{
		for (int i = 0; i < 5; i++) {
			if(portNumbers[i] == port)
				return i;
		}
		return 0;
	}
	//List<Integer> ports = Arrays.asList(portNumbers);
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String key,value;
		Log.e(TAG, "Inside Content Provider Insert");
		deleteData(selection);
		return 1;

	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key,value;
		//Log.e(TAG, "Inside Content Provider Insert");
		key = (String)values.get("key");
		value = (String)values.get("value");
		Log.e(TAG, "Received Key = " + key + " Value=" + value);
		insertData(key,value);
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = (Integer.parseInt(portStr) * 2);
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(SERVER_PORT);

		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
		}
		new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		try {
			String reply = new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(portNumbers[(getIndex(myPort) + 1) % 5]), "4" + myPort).get();
			insertKeys(portNumbers[(getIndex(myPort) + 1) % 5],reply);
			reply = new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(portNumbers[(getIndex(myPort) + 4) % 5]), "5").get();
			insertKeys(portNumbers[(getIndex(myPort) + 4) % 5],reply);
			reply = new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(portNumbers[(getIndex(myPort) + 3) % 5]), "5").get();
			insertKeys(portNumbers[(getIndex(myPort) + 3) % 5],reply);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		if(selection.contains("@"))
			return stringToCursor(readFromLocalDataStore(),new MatrixCursor(new String[]{"key", "value"}));
		else if(selection.contains("*"))
		{
			//MatrixCursor cursor = (MatrixCursor) stringToCursor(readFromLocalDataStore(),new MatrixCursor(new String[]{"key", "value"}));
			return stringToCursor(readFromAllDataStore(2+"*"),new MatrixCursor(new String[]{"key", "value"}));
		}
		else
			return stringToCursor(queryData(selection), new MatrixCursor(new String[]{"key", "value"}));
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private void handleInsertMsg(int coOrdinate, String key, String value) {
		FileOutputStream outputStream;
		Log.e(TAG, "Inserting Key :" + key + " Value: " + value);
		try {
			String fileName = coOrdinate + "_" + key;
			outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
			outputStream.write(value.getBytes());
			outputStream.close();
		} catch (Exception e) {
			Log.v(TAG, "Exception in Writing onto Disk");
		}
		// Log.v(TAG, "Insertion Successful");
	}

	private int deleteDataItem(int coordinator,String key) {
				File dir = getContext().getFilesDir();
				for(File file: dir.listFiles())
					if (!file.isDirectory() && file.getName().equals(coordinator + "_" + key))
						file.delete();
				return 1;
	}

/*	private int deleteFromLocalDataStore() {
		File dir = getContext().getFilesDir();
		for(File file: dir.listFiles())
			if (!file.isDirectory())
				file.delete();
		return 1;
	}

	private int deleteFromAllDataStore(String msg) {
			//Log.e(TAG, "Still not found position..Forwarding Req to Successor");
			String reply = "";
			int receiverPort = Integer.parseInt(msg.substring(2,7));
			//Log.e(TAG, "Sending read Request to :" + successorPort + " From: " + myPort);
				Socket socket = null;
			for (int i = (myPort+1)%5; i!= myPort  ; )
			{
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							i);
					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					PrintWriter pw = null;
					pw = new PrintWriter(socket.getOutputStream(), true);
					pw.println(msg);
					i = (myPort+1)%5;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			deleteFromLocalDataStore();
			return  1;
		}*/

	private void sendMsg(int receivePort, String msg) {
		Socket socket;
		try {
			//Log.e(TAG, "receivePort " + msg+  "to " + receivePort);
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					receivePort);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintWriter pw = null;
			pw = new PrintWriter(socket.getOutputStream(), true);
			pw.println(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void insertData(String key, String value)
	{
		int coOrdinate = findCoordinator(key);
		Log.e(TAG, "Coordinator = " + coOrdinate);
		int firstReplica = portNumbers[(getIndex(coOrdinate)+1) % 5];
		Log.e(TAG, "firstReplica = " + firstReplica);
		int secondReplica = portNumbers[(getIndex(coOrdinate)+2) % 5];
		Log.e(TAG, "secondReplica = " + secondReplica);
		try {
			if(myPort == coOrdinate)
				handleInsertMsg(coOrdinate,key,value);
			else
			{
				new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(coOrdinate),"1" + coOrdinate + key + "_" + value).get();
			}
			if(myPort == firstReplica)
				handleInsertMsg(coOrdinate,key,value);
			else
			{
				//sendMsg(firstReplica,"1" + coOrdinate + key + "_" + value);
				new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(firstReplica),"1" + coOrdinate + key + "_" + value).get();

			}
			if(myPort == secondReplica)
				handleInsertMsg(coOrdinate,key,value);
			else
			{
				//sendMsg(secondReplica,"1" + coOrdinate + key + "_" + value);
				new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(secondReplica),"1" + coOrdinate + key + "_" + value).get();
			}
		}
		catch (InterruptedException e) {
		e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	private String queryData(String key)
	{
		Log.e(TAG, "Received Query Request  for Key = " + key);
		int coOrdinate = findCoordinator(key);

		if(coOrdinate == myPort)
			return findDataItem(String.valueOf(myPort),key);
		String ret = null;
		try {
			Log.e(TAG, "Sending query req to Coordinatior for key" + key);
			ret = new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(coOrdinate),"2" + coOrdinate + key).get();
			Log.e(TAG, "Returned Value from Coordinator for key  = " + key + "is  = "  + ret);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		if(ret != null)
		return ret;
		int firstReplica = (getIndex(coOrdinate)+1) % 5;
		if(firstReplica == myPort)
			return findDataItem(String.valueOf(coOrdinate),key);
		try {
		Log.e(TAG, "Sending query req to First replica for key" + key );
		ret = new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf( portNumbers[firstReplica]),"2" + coOrdinate + key).get();
		Log.e(TAG, "Returned Value from First replica for key  = " + key + "is  = "  + ret);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		Log.e(TAG, "Returned Value from Coordinator = " + ret);
		return ret;
	}

	private void deleteData(String key)
	{
		int coOrdinate = findCoordinator(key);
		int firstReplica = portNumbers[(getIndex(coOrdinate)+1) % 5];
		int secondReplica = portNumbers[(getIndex(coOrdinate)+2) % 5];
		try {
			if (coOrdinate == myPort)
				deleteDataItem(coOrdinate, key);
			else
				new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(coOrdinate), "3" + coOrdinate + key).get();
			if (firstReplica == myPort)
				deleteDataItem(coOrdinate, key);
			else
				new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(firstReplica), "3" + coOrdinate + key).get();
			if (secondReplica == myPort)
				deleteDataItem(coOrdinate, key);
			else
				new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(secondReplica), "3" + coOrdinate + key).get();
		}
		catch (InterruptedException e) {
		e.printStackTrace();
		} catch (ExecutionException e) {
		e.printStackTrace();
	}
	}

	private String readFromAllDataStore(String msg) {
		String reply  = null;
		String answer = "";
		String localdataStore = readFromLocalDataStore();
		if(localdataStore != null )
			answer = localdataStore;
		Socket socket = null;
			try {
				for (int i = (getIndex(myPort)+1)%5; i!= getIndex(myPort); ) {
					reply = new ClientTaskValue().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(portNumbers[i]), msg).get();
					Log.e(TAG, "Received Reply for * = " + reply);

					if(answer ==  null)
						answer = reply;
					else if(reply != null )
						answer =  answer + "_"+ reply;
					i = (i+1)% 5;
				}
			}catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		Log.e(TAG, "Answer for * = " + answer);
		return  answer;
	}

	private Cursor stringToCursor(String msg, MatrixCursor cursor)
	{
		//Log.e(TAG,"Inside stringToCursor");
		//Log.e(TAG,"@ Result String After" + msg);
		if(msg == null || msg.equals(""))
			return cursor;
		String [] split =  msg.split("_");
		for (int i = 0; (i+1) < split.length;) {
			String key = split[i];
			i++;
			String value = split[i];
			i++;
			//Log.e(TAG,"Key : " + key + " Value: " + value);
			cursor.newRow().add(key).add(value);
		}
		return cursor;
	}

	private void insertKeys(int coordinator, String keys)
	{
		if(keys == null || keys.equals(""))
			return;
		String [] split =  keys.split("_");
		for (int i = 0; (i+1) < split.length;) {
			String key = split[i];
			i++;
			String value = split[i];
			i++;
			//Log.e(TAG,"Key : " + key + " Value: " + value);
			handleInsertMsg(coordinator,key,value);
		}

	}

	private int findCoordinator(String key)
	{
		try {
			String keyHash = genHash(key);
			//System.out.println("Key Hash " + keyHash);
			if(keyHash.compareTo( genHash(String.valueOf(5562))) > 0 && keyHash.compareTo( genHash(String.valueOf(5556))) < 1)
			{
				return 5556*2;
			}
			else if(keyHash.compareTo( genHash(String.valueOf(5556))) > 0 && keyHash.compareTo( genHash(String.valueOf(5554))) < 1)
			{
				return 5554*2;
			}
			else if(keyHash.compareTo( genHash(String.valueOf(5554))) > 0 && keyHash.compareTo( genHash(String.valueOf(5558))) < 1)
			{
				return 5558*2;
			}
			else if(keyHash.compareTo( genHash(String.valueOf(5558))) > 0 && keyHash.compareTo( genHash(String.valueOf(5560))) < 1)
			{
				return 5560*2;
			}
			else
				return 5562*2;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return 0;
	}

	private  String readFromLocalDataStore()
	{
		//Log.e(TAG,"Inside readFromLocalDataStore..reading from local Data Store");
		File file = getContext().getFilesDir();
		File[] files = file.listFiles();
		if(files.length == 0)
			return null;
		FileInputStream inputStream;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbMain = new StringBuffer();
		for (int j = 0; j < files.length; j++) {
			try {
				inputStream = getContext().openFileInput(files[j].getName());
				int i;
				while ((i = inputStream.read()) != -1) {
					sb.append((char) i);
				}
				//Log.e(TAG,"Key: " + files[j].getName() + " Value: " +sb);
				inputStream.close();
			} catch (Exception e) {
			}
			sbMain.append(files[j].getName().split("_")[1]+ "_");
			sbMain.append(sb + "_");
			sb.setLength(0);
		}
		if(sbMain.length() > 0)
		sbMain.setLength(sbMain.length()-1);
		//Log.e(TAG,"@ Result String before" + sbMain);
		//Log.e(TAG,"Whole Local Store String: " + sbMain);
		return sbMain.toString();
	}

	private  String readFromLocalDataStoreReplica(String coOrdinator)
	{
		//Log.e(TAG,"Inside readFromLocalDataStore..reading from local Data Store");
		File file = getContext().getFilesDir();
		File[] files = file.listFiles();
		if(files.length == 0)
			return null;
		FileInputStream inputStream;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbMain = new StringBuffer();
		for (int j = 0; j < files.length; j++) {
			try {
				if(!files[j].getName().contains(coOrdinator))
					continue;
				inputStream = getContext().openFileInput(files[j].getName());
				int i;
				while ((i = inputStream.read()) != -1) {
					sb.append((char) i);
				}
				//Log.e(TAG,"Key: " + files[j].getName() + " Value: " +sb);
				inputStream.close();
			} catch (Exception e) {
			}
			sbMain.append(files[j].getName().split("_")[1]+ "_");
			sbMain.append(sb + "_");
			sb.setLength(0);
		}
		if(sbMain.length() > 0)
			sbMain.setLength(sbMain.length()-1);
		//Log.e(TAG,"@ Result String before" + sbMain);
		//Log.e(TAG,"Whole Local Store String: " + sbMain);
		return sbMain.toString();
	}

	private String findDataItem(String coordinator, String key) {
		StringBuffer sb = new StringBuffer();
		StringBuffer sbMain = new StringBuffer();
				//Log.e(TAG,"Inside findDataItem");
				MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
				try {
					FileInputStream inputStream;
					Log.e(TAG, "Searching for File : " + coordinator+ "_"+ key);
					inputStream = getContext().openFileInput(coordinator+ "_"+ key);
					if(inputStream == null)
					{
						Log.e(TAG, "File Not Found");
						return null;
					}

					int i;
					while ((i = inputStream.read()) != -1) {
						sb.append((char) i);
					}
					inputStream.close();
				} catch (Exception e) {
					return null;
				}
				sbMain.append(key + "_");
				sbMain.append(sb);
				//Log.e(TAG,"@ Result Before " +sbMain);
				return sbMain.toString();
			}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			int seq_no = 0;
			try {
				while (true) {
					Socket client = null;
					//Log.e(TAG, "Server Socket Created");
					client = serverSocket.accept();
					//Log.e(TAG, "Client Connection accepted");
					BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));
					String msg;

					msg = br.readLine();
					if(msg == null)
						return null;
					String reply;
					if(msg.charAt(0) == '0')
					{}
					else if(msg.charAt(0) == '1')
					{
						Log.e(TAG, "Received Insertion Message Inside Server: " + msg);
                        int coordinator = Integer.parseInt(msg.substring(1,6));
						String split[] = msg.substring(6).split("_");
						handleInsertMsg(coordinator,split[0],split[1]);
					}
					else if(msg.charAt(0) == '2')
					{
							
						if(msg.charAt(1) == '*')
						{
							reply = readFromLocalDataStore();
							PrintWriter pwMain = new PrintWriter(client.getOutputStream(), true);
							pwMain.println(reply);
						}
						else
						{
							//String key =  msg.substring(2,2+Character.getNumericValue(msg.charAt(1)));
							String key =  msg.substring(6);
							reply =findDataItem(msg.substring(1,6),key);
							PrintWriter pwMain = new PrintWriter(client.getOutputStream(), true);
							pwMain.println(reply);
						}
						//insert msg

					}
					else if(msg.charAt(0) == '3')
					{
						int coordinator = Integer.parseInt(msg.substring(1,6));
						String key =  msg.substring(6);
						deleteDataItem(coordinator,key);
					}

					else if(msg.charAt(0) == '4')
					{
						reply = readFromLocalDataStoreReplica(msg.substring(1,6));
						PrintWriter pwMain = new PrintWriter(client.getOutputStream(), true);
						pwMain.println(reply);
					}
					else if(msg.charAt(0) == '5')
					{
						reply = readFromLocalDataStoreReplica(String.valueOf(myPort));
						PrintWriter pwMain = new PrintWriter(client.getOutputStream(), true);
						pwMain.println(reply);
					}
				}
			}catch (IOException e) {
				e.printStackTrace();
			}

			return null;
		}
	}
/*
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(msgs[0]));
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String inputLine;
				Log.e(TAG, "Sending " + msgs[1] +  "to " + msgs[0]);
				PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
				pw.println(msgs[1]);
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG,e.getMessage());
				Log.e(TAG, "ClientTask socket IOException");

			}
			return null;
		}

	}*/

	private class ClientTaskValue extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(msgs[0]));
				socket.setSoTimeout(500);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String inputLine;
				//Log.e(TAG, "Sending " + msgs[1] +  "to " + msgs[0]);
				PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
				pw.println(msgs[1]);
				inputLine = in.readLine();
				if(inputLine != null)
					return inputLine;

			}catch (SocketTimeoutException e)
			{
				Log.e(TAG, "Socket Timeout happened");
				return null;

			}
			catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");

			}
			return null;
		}

	}
}
