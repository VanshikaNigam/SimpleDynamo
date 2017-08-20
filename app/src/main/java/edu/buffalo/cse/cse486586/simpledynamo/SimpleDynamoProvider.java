package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	String ports[] = {"5554", "5556", "5558", "5560", "5562"};
	static final int SERVER_PORT = 10000;
	String type_of_work[]={"New","SendInfo","Replication_1","Replication_2","Replication Insert Forward","Query_Forward","Got value","Retrieve","Delete","Alive","Requesting Successor","Requesting Predecessor 1","Requesting Predecessor 2"};
	String myPort = null;
	String portStr = null;
	String key;
	String value;
	String pred_hash;
	String succ_hash;
	String hashed_port;
	String dead_port=null;
	boolean dead_state=false;
	String predeccessor,successor_1,successor_2,predeccessor2;
	//ConcurrentHashMap<String, String> port_and_hash = new ConcurrentHashMap<String, String>();
	List<String> node_id = new ArrayList<String>(); //for genhash of ports
	List<String> store = new ArrayList<String>(); // for port numbers

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String selection_hashed="";
		try {
			selection_hashed = genHash(selection);
			pred_hash=genHash(predeccessor);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		if(selection.equals("@"))
		{
			String files[] = getContext().fileList();
			for(String list:files)
				getContext().deleteFile(list);
		}

		else if ((selection_hashed.compareTo(pred_hash) > 0) && selection_hashed.compareTo(hashed_port) < 0)
		{
			getContext().deleteFile(selection);
		}

		else if (isFirstNode() && (selection_hashed.compareTo(pred_hash) > 0 || selection_hashed.compareTo(hashed_port) < 0))
		{
			getContext().deleteFile(selection);
		}
		else {
			String files[] = getContext().fileList();
			for(String list:files)
			{
				if (selection.equals(list))
					getContext().deleteFile(list);
			}
			String msg_forward_delete = portStr + "#" + type_of_work[8] + "#" + successor_1 + "#" + selection;

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_forward_delete);
			Log.d("Delete", msg_forward_delete);

		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	private boolean isFirstNode(){

		try {
			pred_hash = genHash(predeccessor);
			succ_hash = genHash(successor_1);
		}
		catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}

		if(pred_hash.compareTo(hashed_port)>0 && succ_hash.compareTo(hashed_port)>0)
			return true;
		return false;
	}
	@Override
	public synchronized Uri  insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		key = values.getAsString("key");
		FileOutputStream outputStream;
		value=values.getAsString("value");

		String hashed_key;

		try
		{

			hashed_key=genHash(key);
			pred_hash=genHash(predeccessor);
			Log.d(TAG,"insert key "+key+" value = "+value+" hashed_key = "+hashed_key);


			int i=0;
				Log.d(TAG,"Inserting store.size() "+store.size());
				for (i=1;i<store.size();i++)
				{
					String first=genHash(store.get(i));
					String prev=genHash(store.get(i-1));
					Log.d(TAG,"insert prev hash = "+prev+" curr hash "+first+" and key hash = "+hashed_key+" key "+key);
					Log.d(TAG," insert first.compareTo(hashed_key) = "+first.compareTo(hashed_key)+" prev.compareTo(hashed_key) ="+prev.compareTo(hashed_key));
					if(first.compareTo(hashed_key)>0 && prev.compareTo(hashed_key)<0)
					{
						Log.d(TAG,"Inserting 1 key hash="+hashed_key+"to port= "+store.get(i)+" with hash = "+first);
						String msg_to_forward = store.get(i) + "#" + type_of_work[4] + "#" + key + "#" + value+"#"+i;
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_forward);
						break;
					}

				}
				if(i==store.size())
				{
					Log.d(TAG,"Inserting 2 key hash="+hashed_key+"to port= "+store.get(0)+" with hash = "+genHash(store.get(0)));
					String msg_to_forward = store.get(0) + "#" + type_of_work[4] + "#" + key + "#" + value+"#"+0;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_forward);
				}



		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		hashed_port="";
		for (int i = 0; i < ports.length; i++) {
			try {
				node_id.add(genHash(ports[i]));

				Collections.sort(node_id);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		store.add("5562");
		store.add("5556");
		store.add("5554");
		store.add("5558");
		store.add("5560");

		for(int i =0;i<store.size();i++)
		{
			Log.d("Store",store.get(i));
		}


		if(portStr.equals("5562"))
		{

			predeccessor="5560";
			predeccessor2="5558";
			successor_1="5556";
			successor_2="5554";
		}
		if(portStr.equals("5556"))
		{
			predeccessor="5562";
			predeccessor2="5560";
			successor_1="5554";
			successor_2="5558";
		}
		if(portStr.equals("5554"))
		{
			predeccessor="5556";
			predeccessor2="5562";
			successor_1="5558";
			successor_2="5560";
		}
		if(portStr.equals("5558"))
		{
			predeccessor="5554";
			predeccessor2="5556";
			successor_1="5560";
			successor_2="5562";
		}
		if(portStr.equals("5560"))
		{
			predeccessor="5558";
			predeccessor2="5554";
			successor_1="5562";
			successor_2="5556";
		}


		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			hashed_port = genHash(portStr);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


			//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, type_of_work[0]);

			//int current_index=store.indexOf(portStr);
			String msgtosend = portStr + "#" + type_of_work[9]+"#"+predeccessor+"#"+predeccessor2+"#"+successor_1;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);


		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		String value_msg = " ";
		String selection_hashed = " ";
		Log.d(TAG,"QueryReceived "+selection);

		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});

		try {
			pred_hash = genHash(predeccessor);
			if(!selection.equals("*")||(!selection.equals("@")))
			selection_hashed = genHash(selection);
		}catch(NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		if (selection.equals("@")||selection.equals("\"@\"")) {

			Log.d(TAG,"query inside @");
			String files[] = getContext().fileList();
			for (String list : files) {
				try {
					FileInputStream fin = getContext().openFileInput(list);
					Log.d(TAG,"query inside @ key:" + list);
					InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
					BufferedReader br = new BufferedReader(isr);
					value_msg = br.readLine();
					Log.d(TAG,"query inside @ Query condition @ "+selection);
					Log.d(TAG,"query inside @ Value: "+value_msg);
					cursor.addRow(new Object[]{list, value_msg});
					fin.close();
					isr.close();
					br.close();
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				//Log.d(TAG, "what" + String.valueOf(cursor));

			}
			return cursor;
		}else if (selection.equals("*") || selection.equals("\"*\"")) {
			try {
				String files[] = getContext().fileList();
				for (String list : files) {

					FileInputStream fin = getContext().openFileInput(list);
					//System.out.println("key:" + list);
					InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
					BufferedReader br = new BufferedReader(isr);
					value_msg = br.readLine();
					//Log.d("Query condition @ 4 *", selection);
					//Log.d("Value:", value_msg);
					cursor.addRow(new Object[]{list, value_msg});
				}
				String originating_port = portStr;
				String msg_for_star = originating_port + "#" + type_of_work[7] + "#" + successor_1 + "#" + "*";
				String rp[]={REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
				for (int i=0;i<rp.length;i++) {
					if (!originating_port.equals(rp[i])) {
						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_for_star);
						//try  catch for failure
						try{
						Log.d(TAG,"query Querystar rp[i] = " +rp[i]);
						msg_for_star = originating_port + "#" + type_of_work[7] + "#" + rp[i] + "#" + "*";
						String all_data = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_for_star).get();
						String temp[] = all_data.split("#");
						String keys[] = temp[2].split(",");
						String values[] = temp[3].split(",");
						for (int j = 0; j < keys.length; j++) {
							cursor.addRow(new Object[]{keys[j], values[j]});
						}
					}catch (Exception e)
						{
							e.printStackTrace();
						}
					}

				}
				return cursor;

			} catch (Exception e) {
				e.printStackTrace();
			}

		}


		else if ((selection_hashed.compareTo(pred_hash) > 0) && (selection_hashed.compareTo(hashed_port) < 0)) {

			try {

				FileInputStream fin = getContext().openFileInput(selection);
				InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
				BufferedReader br = new BufferedReader(isr);
				value_msg = br.readLine();
				Log.d(TAG,"Query condition @" + hashed_port);
				Log.d(TAG,"query Value:" + value_msg);
				fin.close();
				isr.close();
				br.close();

			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			//Log.d(TAG, "what" + String.valueOf(cursor));
			cursor.addRow(new Object[]{selection, value_msg});
			return cursor;
		} else if (isFirstNode() && (selection_hashed.compareTo(pred_hash) > 0 || selection_hashed.compareTo(hashed_port) < 0)) {
			try {

				FileInputStream fin = getContext().openFileInput(selection);
				InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
				BufferedReader br = new BufferedReader(isr);
				value_msg = br.readLine();
				Log.d(TAG,"query Query condition @ " + hashed_port);
				Log.d(TAG,"Value: "+ value_msg);
				fin.close();
				isr.close();
				br.close();

			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			//Log.d(TAG, "what" + String.valueOf(cursor));
			cursor.addRow(new Object[]{selection, value_msg});
			return cursor;

		}

		else {
			try{
				int i=0;
				for(i=1;i<store.size();i++)
				{
					String first=genHash(store.get(i));
					String prev=genHash(store.get(i-1));
					if ((first.compareTo(selection_hashed)>0)&&(prev.compareTo(selection_hashed)<0))
					{
						String msg_to_foward = store.get(i) + "#" + type_of_work[5] + "#" + successor_1 + "#" + selection;
						Log.d(TAG,"query to port "+ store.get(i)+" to key "+ selection);
						String s = null;
						s = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_foward).get();
						Log.d(TAG,"query Message received" + s);
						String temp[] = s.split("#");
						cursor.addRow(new Object[]{temp[3], temp[4]});
						break;
					}
				}
				if(i==store.size())
				{
					String msg_to_foward = store.get(0) + "#" + type_of_work[5] + "#" + successor_1 + "#" + selection;
					Log.d(TAG,"query to port "+ store.get(0)+" to key "+ selection);
					String s = null;
					s = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_foward).get();
					Log.d(TAG," query Message received "+ s);
					String temp[] = s.split("#");
					cursor.addRow(new Object[]{temp[3], temp[4]});
				}
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}catch(NoSuchAlgorithmException e){
				e.printStackTrace();
			}
			return  cursor;
		}

		return null;
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			String messages = null;
			//String msg[]=messages.split("#");
			Log.d(TAG, "I am here");
			try {
				while (true) {

					Socket socket = serverSocket.accept();
					InputStream in = socket.getInputStream();
					DataInputStream data = new DataInputStream(in);
					messages = data.readUTF();
						//Log.d("successful",messages);

					OutputStream out = socket.getOutputStream();
					DataOutputStream dout = new DataOutputStream(out);
					String msg[]=messages.split("#");


					 if (msg[1].equals(type_of_work[4])) {

						Log.d("Forward to successor:",msg[2]);
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(msg[2], Context.MODE_PRIVATE);
						Log.d(TAG,"Replication Insertion "+msg[2]);
						outputStream.write(msg[3].getBytes());
						outputStream.close();
						dout.writeUTF("OK");
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

					}

					else if (msg[1].equals(type_of_work[5])) {
						//calling query

						Log.d("forward condition", msg[3]);
						Uri.Builder uriBuilder = new Uri.Builder();
						uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
						uriBuilder.scheme("content");
						Uri uri = uriBuilder.build();


						Cursor query_search = query(uri, null, msg[3], null, null);
						//query_search.moveToFirst();

						int keyIndex = query_search.getColumnIndex("key");
						int valueIndex = query_search.getColumnIndex("value");

						Log.d("Sending the query", String.valueOf(keyIndex));
						Log.d("Sending the query", String.valueOf(valueIndex));

						query_search.moveToFirst();

						String query_search_key = query_search.getString(keyIndex);
						String query_search_value = query_search.getString(valueIndex);

						Log.d("Sending the query", value);
						query_search.close();

						String msg_after_selection = portStr + "#" + type_of_work[6] + "#" + successor_1 + "#" + query_search_key + "#" + query_search_value;
						Log.d("final message", msg_after_selection);
						dout.writeUTF(msg_after_selection);
					}

					else if(msg[1].equals(type_of_work[7]))
					{
						String value_msg;
						MatrixCursor cursor_s = new MatrixCursor(new String[]{"key", "value"});
						String files[] = getContext().fileList();
						for (String list : files) {
							try {
								FileInputStream fin = getContext().openFileInput(list);
								System.out.println("key:" + list);
								InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
								BufferedReader br = new BufferedReader(isr);
								value_msg = br.readLine();
								Log.d("Query condition @", messages);
								Log.d("Value:", value_msg);
								cursor_s.addRow(new Object[]{list, value_msg});
								fin.close();
								isr.close();
								br.close();
							} catch (UnsupportedEncodingException e) {
								e.printStackTrace();
							} catch (FileNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
							}
							String keys="";
							String values="";
							while(cursor_s.moveToNext()){
								keys = keys + cursor_s.getString(cursor_s.getColumnIndex("key")) + ",";
								values = values + cursor_s.getString(cursor_s.getColumnIndex("value")) + ",";
							}

							Log.d("KeysBefore",keys);
							Log.d("ValuesBefore",values);

							//keys = keys.substring(0,keys.length()-1);
							//values = values.substring(0,values.length()-1);

							Log.d("KeysAfter",keys);
							Log.d("ValuesAfter",values);

							String msg_final=msg[0]+"#"+msg[1]+"#"+keys+"#"+values;
							//String msg_final=msg[0]+"#"+msg[1]+"#"+"*";
							dout.writeUTF(msg_final);
						Log.d(TAG,"output for *  sent");
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

					}


					else if (msg[1].equals(type_of_work[8])){
						Log.d("Deletion started",messages);
						Uri.Builder uriBuilder = new Uri.Builder();
						uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
						uriBuilder.scheme("content");
						Uri uri = uriBuilder.build();
						Log.d("Deletion Server",msg[3]);
						delete(uri,msg[3],null);

					}

					else if(msg[1].equals(type_of_work[10]))
					{
						String value_msg;
						String mypt=genHash(msg[0]);
						String pred=genHash(msg[2]);
						MatrixCursor cursor_s = new MatrixCursor(new String[]{"key", "value"});
						String files[] = getContext().fileList();
						for (String list : files) {
							try {
								FileInputStream fin = getContext().openFileInput(list);
								//System.out.println("key:" + list);
								InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
								BufferedReader br = new BufferedReader(isr);
								//value_msg = br.readLine();

								if((genHash(list).compareTo(pred)>0)&&(genHash(list).compareTo(mypt)<0))
								{
									Log.d(TAG,"Type_of_work 10 condition1 "+genHash(list)+" "+pred+" "+mypt);
									value_msg = br.readLine();
									cursor_s.addRow(new Object[]{list, value_msg});
								}
								else if (isFirstNode() && (genHash(list).compareTo(pred) > 0 || genHash(list).compareTo(mypt) < 0))
								{
									Log.d(TAG,"Type_of_work 10 condition2 "+genHash(list)+" "+pred+" "+mypt);
									value_msg = br.readLine();
									cursor_s.addRow(new Object[]{list, value_msg});
								}
								else
								{
									Log.d(TAG,"Genhashlist1 "+genHash(list));
								}

								Log.d("Query condition @", messages);
								//Log.d("Value:", value_msg);
								//cursor_s.addRow(new Object[]{list, value_msg});
								fin.close();
								isr.close();
								br.close();
							} catch (UnsupportedEncodingException e) {
								e.printStackTrace();
							} catch (FileNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						String keys="";
						String values="";
						while(cursor_s.moveToNext()){
							keys = keys + cursor_s.getString(cursor_s.getColumnIndex("key")) + ",";
							values = values + cursor_s.getString(cursor_s.getColumnIndex("value")) + ",";
						}

						Log.d("KeysBefore",keys);
						Log.d("ValuesBefore",values);

						//keys = keys.substring(0,keys.length()-1);
						//values = values.substring(0,values.length()-1);

						Log.d("KeysAfter",keys);
						Log.d("ValuesAfter",values);

						String msg_final=msg[0]+"#"+msg[1]+"#"+keys+"#"+values;

						dout.writeUTF(msg_final);
						Log.d(TAG,"SERVER : output for alive selected values sent");
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

					else if(msg[1].equals(type_of_work[11]))
					 {
						 String value_msg;
						 String pred1=genHash(msg[2]);
						 String pred2=genHash(msg[4]);
						 MatrixCursor cursor_s = new MatrixCursor(new String[]{"key", "value"});
						 String files[] = getContext().fileList();
						 for (String list : files) {
							 try {
								 FileInputStream fin = getContext().openFileInput(list);
								 System.out.println("key:" + list);
								 InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
								 BufferedReader br = new BufferedReader(isr);
								 //value_msg = br.readLine();

								 if((genHash(list).compareTo(pred2)>0)&&(genHash(list).compareTo(pred1)<0))
								 {
									 Log.d(TAG,"Type_of_work 11 condition1 "+genHash(list)+" "+pred1+" "+pred2);
									 value_msg = br.readLine();
									 cursor_s.addRow(new Object[]{list, value_msg});
								 }
								 else if (isFirstNode() && (genHash(list).compareTo(pred2) > 0 || genHash(list).compareTo(pred1) < 0))
								 {
									 Log.d(TAG,"Type_of_work 11 condition2 "+genHash(list)+" "+pred1+" "+pred2);
									 value_msg = br.readLine();
									 cursor_s.addRow(new Object[]{list, value_msg});
								 }

								 else
								 {
									 Log.d(TAG,"Genhashlist2 "+genHash(list));
								 }
								 Log.d("Query condition @", messages);
								 //Log.d("Value:", value_msg);
								 //cursor_s.addRow(new Object[]{list, value_msg});
								 fin.close();
								 isr.close();
								 br.close();
							 } catch (UnsupportedEncodingException e) {
								 e.printStackTrace();
							 } catch (FileNotFoundException e) {
								 e.printStackTrace();
							 } catch (IOException e) {
								 e.printStackTrace();
							 }
						 }
						 String keys="";
						 String values="";
						 while(cursor_s.moveToNext()){
							 keys = keys + cursor_s.getString(cursor_s.getColumnIndex("key")) + ",";
							 values = values + cursor_s.getString(cursor_s.getColumnIndex("value")) + ",";
						 }

						 Log.d("KeysBefore",keys);
						 Log.d("ValuesBefore",values);

						 //keys = keys.substring(0,keys.length()-1);
						 //values = values.substring(0,values.length()-1);

						 Log.d("KeysAfter",keys);
						 Log.d("ValuesAfter",values);

						 String msg_final=msg[0]+"#"+msg[1]+"#"+keys+"#"+values;

						 dout.writeUTF(msg_final);
						 Log.d(TAG,"SERVER : output for alive selected values sent");
						 try {
							 Thread.sleep(100);
						 } catch (InterruptedException e) {
							 e.printStackTrace();
						 }
					 }

					 else if(msg[1].equals(type_of_work[12]))
					 {
						 String value_msg;
						 String pred1=genHash(msg[4]);
						 String pred2=genHash(msg[5]);
						 MatrixCursor cursor_s = new MatrixCursor(new String[]{"key", "value"});
						 String files[] = getContext().fileList();
						 for (String list : files) {
							 try {
								 FileInputStream fin = getContext().openFileInput(list);
								 System.out.println("key:" + list);
								 InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
								 BufferedReader br = new BufferedReader(isr);
								 //value_msg = br.readLine();

								 if((genHash(list).compareTo(pred2)>0)&&(genHash(list).compareTo(pred1)<0))
								 {
									 Log.d(TAG,"Type_of_work 12 condition1 "+genHash(list)+" "+pred1+" "+pred2);
									 value_msg = br.readLine();
									 cursor_s.addRow(new Object[]{list, value_msg});
								 }
								 else if (isFirstNode() && (genHash(list).compareTo(pred2) > 0 || genHash(list).compareTo(pred1) < 0))
								 {
									 Log.d(TAG,"Type_of_work 12 condition1 "+genHash(list)+" "+pred1+" "+pred2);
									 value_msg = br.readLine();
									 cursor_s.addRow(new Object[]{list, value_msg});
								 }
								 else{
									 Log.d(TAG,"Genhashlist3 "+genHash(list));
								 }

								 Log.d("Query condition @", messages);
								// Log.d("Value:", value_msg);
								 //cursor_s.addRow(new Object[]{list, value_msg});
								 fin.close();
								 isr.close();
								 br.close();
							 } catch (UnsupportedEncodingException e) {
								 e.printStackTrace();
							 } catch (FileNotFoundException e) {
								 e.printStackTrace();
							 } catch (IOException e) {
								 e.printStackTrace();
							 }
						 }
						 String keys="";
						 String values="";
						 while(cursor_s.moveToNext()){
							 keys = keys + cursor_s.getString(cursor_s.getColumnIndex("key")) + ",";
							 values = values + cursor_s.getString(cursor_s.getColumnIndex("value")) + ",";
						 }

						 Log.d("KeysBefore",keys);
						 Log.d("ValuesBefore",values);

						 //keys = keys.substring(0,keys.length()-1);
						 //values = values.substring(0,values.length()-1);

						 Log.d("KeysAfter",keys);
						 Log.d("ValuesAfter",values);

						 String msg_final=msg[0]+"#"+msg[1]+"#"+keys+"#"+values;

						 dout.writeUTF(msg_final);
						 Log.d(TAG,"SERVER : output for alive selected values sent");
						 try {
							 Thread.sleep(100);
						 } catch (InterruptedException e) {
							 e.printStackTrace();
						 }
					 }



				}

			} catch (IOException e) {
				e.printStackTrace();
			}
			catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			return null;
		}

		/*protected void onProgressUpdate(String... strings) {

			//String port_info[]=strings[0].split("#");
			//strings[0]=type_of_work[1]+"#"+strings[0];
			//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0]);
		}*/
		}


	private class ClientTask extends AsyncTask<String,String,String>
	{
		protected String doInBackground(String... msgs)
		{
			String messages=msgs[0];
			String msg_split[]=msgs[0].split("#");
			try
			{

				 if (msg_split[1].equals(type_of_work[4])) {

					int i = Integer.parseInt(msg_split[4]);//i
					Socket socket;
					OutputStream out;
					DataOutputStream d;
					InputStream in;
					DataInputStream din;
					String ack;
					String msg_to_insert = msgs[0];

					try {
						Log.d(TAG, "clienttask1A sending to " + msg_split[0] + " for key = " + msg_split[2]);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(store.get(i)) * 2);
						out = socket.getOutputStream();
						d = new DataOutputStream(out);
						d.writeUTF(msg_to_insert);
						Log.v(TAG, " clienttask2A sent to " + Integer.parseInt(store.get(i)) + " for key = " + msg_split[2]);

						in = socket.getInputStream();
						din = new DataInputStream(in);
						ack = din.readUTF();
						Log.v(TAG, " clienttask3A received by " + Integer.parseInt(store.get(i)) + " for key = " + msg_split[2]);
						if (ack.equals("OK"))
							socket.close();
						out.close();
						d.close();
						in.close();
						din.close();
					}catch (SocketTimeoutException s) {
						s.printStackTrace();
						dead_port = store.get(i);
						dead_state = true;
						Log.d(TAG, "Inside Insert 1 :DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					} catch (IOException e) {
						e.printStackTrace();
						dead_port = store.get(i);
						dead_state = true;
						Log.d(TAG, "Inside Insert 1:DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					}

					try {


						i = (i + 1) % 5;
						Log.d(TAG, "Replication 1B clienttask sending to " + msg_split[0] + " for key = " + msg_split[2]);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(store.get(i)) * 2);
							msg_to_insert = msgs[0];
						out = socket.getOutputStream();
						d = new DataOutputStream(out);
						d.writeUTF(msg_to_insert);
						Log.v(TAG, " clienttask2B sent to " + Integer.parseInt(store.get(i)) + " for key = " + msg_split[2]);
						in = socket.getInputStream();
						din = new DataInputStream(in);
						ack = din.readUTF();
						Log.v(TAG, " clienttask3B received by " + Integer.parseInt(store.get(i)) + " for key = " + msg_split[2]);
						if (ack.equals("OK"))
							socket.close();
						out.close();
						d.close();
						in.close();
						din.close();
					}catch (SocketTimeoutException s) {
						s.printStackTrace();
						dead_port = store.get(i);
						dead_state = true;
						Log.d(TAG, "Inside Insert 2 : DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					} catch (IOException e) {
						e.printStackTrace();
						dead_port = store.get(i);
						dead_state = true;
						Log.d(TAG, "Inside Insert 2 : DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					}

					try {

						i  = (i + 1) % 5;
						Log.d(TAG, " Replication 2C clienttask sending to " + msg_split[0] + " for key = " + msg_split[2]);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(store.get(i)) * 2);
						msg_to_insert = msgs[0];
						out = socket.getOutputStream();
						d = new DataOutputStream(out);
						d.writeUTF(msg_to_insert);
						Log.v(TAG, " clienttask2C sent to " + Integer.parseInt(store.get(i)) + " for key = " + msg_split[2]);
						in = socket.getInputStream();
						din = new DataInputStream(in);
						ack = din.readUTF();
						Log.v(TAG, " clienttask3C received by " + Integer.parseInt(store.get(i)) + " for key = " + msg_split[2]);
						if (ack.equals("OK"))
							socket.close();
						out.close();
						d.close();
						in.close();
						din.close();
					}catch (SocketTimeoutException s) {
						s.printStackTrace();
						dead_port = store.get(i);
						dead_state = true;
						Log.d(TAG, "Inside Insert : 3 DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					} catch (IOException e) {
						e.printStackTrace();
						dead_port = store.get(i);
						dead_state = true;
						Log.d(TAG, "Inside Insert : 3 DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					}


				}



				else if (msg_split[1].equals(type_of_work[5])){

					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(msg_split[0]) * 2);

						String msg_to_insert = msgs[0];
						Log.v(TAG, "SendingForwardRequest" + msg_to_insert + "FFF" + messages);

						OutputStream out = socket.getOutputStream();
						DataOutputStream d = new DataOutputStream(out);
						d.writeUTF(msg_to_insert);

						String ack;
						//String msg_to_query;

						InputStream in = socket.getInputStream();
						DataInputStream din = new DataInputStream(in);

						ack = din.readUTF();

						Log.d("Replying back", ack);

						if (ack.equals("OK"))
							socket.close();
						else {
							socket.close();
							return ack;

						}

						out.close();
						d.close();
						in.close();
						din.close();
					}catch (SocketTimeoutException s) {
						s.printStackTrace();
						dead_port = msg_split[0];
						dead_state = true;
						Log.d(TAG, "Inside Query 1 :DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					} catch (SocketException s) {
						s.printStackTrace();
						dead_port = msg_split[0];
						dead_state = true;
						Log.d(TAG, "Inside Query 1 DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					} catch (IOException e) {
						e.printStackTrace();
						dead_port = msg_split[0];
						dead_state = true;
						Log.d(TAG, "Inside Query 1 DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					}

					if (dead_state == true) {

						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(msg_split[2]) * 2);


							String msg_to_query = msgs[0];
							Log.v(TAG, "SendingForwardWhenDead" + msg_to_query + "DEAD" + messages);

							OutputStream out = socket.getOutputStream();
							DataOutputStream d = new DataOutputStream(out);
							d.writeUTF(msg_to_query);

							String ack;
							//String msg_to_query;

							InputStream in = socket.getInputStream();
							DataInputStream din = new DataInputStream(in);

							ack = din.readUTF();

							Log.d(TAG, "Replying back from successor 1" + ack);

							if (ack.equals("OK"))
								socket.close();
							else {
								socket.close();
								return ack;

							}

							out.close();
							d.close();
							in.close();
							din.close();
						} catch (SocketTimeoutException s) {
							s.printStackTrace();
							dead_port = msg_split[0];
							dead_state = true;
							Log.d(TAG, "DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
						} catch (SocketException s) {
							s.printStackTrace();
							dead_port = msg_split[0];
							dead_state = true;
							Log.d(TAG, "DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
						} catch (IOException e) {
							e.printStackTrace();
							dead_port = msg_split[0];
							dead_state = true;
							Log.d(TAG, "DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
						}

					}

				}


				else if (msg_split[1].equals(type_of_work[7])) {
					try {
						int port = Integer.parseInt(msg_split[2]);

						Log.d(TAG, "type_of_work sending to port " + port);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);

						String msg_to_insert = msgs[0];
						Log.v(TAG, "AllDataRetrieved" + msg_to_insert + "RRR");

						OutputStream out = socket.getOutputStream();
						DataOutputStream d = new DataOutputStream(out);
						d.writeUTF(msg_to_insert);

						String ack;

						InputStream in = socket.getInputStream();
						DataInputStream din = new DataInputStream(in);

						ack = din.readUTF();
						socket.close();

						out.close();
						d.close();
						in.close();
						din.close();
						return ack;
					}catch (SocketTimeoutException s) {
						s.printStackTrace();
						dead_port = msg_split[2];
						dead_state = true;
						Log.d(TAG, "DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					} catch (SocketException s) {
						s.printStackTrace();
						dead_port = msg_split[2];
						dead_state = true;
						Log.d(TAG, "DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					} catch (IOException e) {
						e.printStackTrace();
						dead_port = msg_split[2];
						dead_state = true;
						Log.d(TAG, "DEAD PORT" + dead_port + "DEAD STATE" + dead_state);
					}

				}
				else if (msg_split[1].equals(type_of_work[8])) {
					try {
						int port = Integer.parseInt(msg_split[2]) * 2;

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								port);

						String msg_to_delete = msgs[0];
						Log.v(TAG, "AllDatadeleted" + msg_to_delete + "DDD");

						OutputStream out = socket.getOutputStream();
						DataOutputStream d = new DataOutputStream(out);
						d.writeUTF(msg_to_delete);

						String ack;

						InputStream in = socket.getInputStream();
						DataInputStream din = new DataInputStream(in);

						ack = din.readUTF();

						if (ack.equals("OK"))
							socket.close();
						out.close();
						d.close();
						in.close();
						din.close();

					}catch (IOException e)
					{
						e.printStackTrace();
					}
				}

				else if (msg_split[1].equals(type_of_work[9]))
				{
					//making socket with successor
					MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
					Socket socket;
					OutputStream out;
					DataOutputStream d;
					InputStream in;
					DataInputStream din;
					String ack;
					//String msg_to_insert = msgs[0];
					try {

						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(successor_1) * 2);
						String msg_request = portStr+"#"+type_of_work[10]+"#"+predeccessor+"#"+successor_1;
						//Log.v(TAG, "Retreive back when alive" + msg_request + "ALIVE" + messages);

						 out = socket.getOutputStream();
						 d = new DataOutputStream(out);
						d.writeUTF(msg_request);

						in = socket.getInputStream();
						din = new DataInputStream(in);

						ack = din.readUTF();
						Log.d(TAG, "Replying back from successor 1 to alive avd" + ack);

						//if (ack.equals("OK") || ack.equals(null))
						//	socket.close();
						//else {
							socket.close();
							String temp[] = ack.split("#");
							String keys[] = temp[2].split(",");
							String values[] = temp[3].split(",");
							for (int j = 0; j < keys.length; j++) {
								cursor.addRow(new Object[]{keys[j], values[j]});
						//	}

						}
						out.close();
						d.close();
						in.close();
						din.close();

					} catch (IOException e) {
						e.printStackTrace();
					}

					try{
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(predeccessor) * 2);
						String msg_request = portStr+"#"+type_of_work[11]+"#"+predeccessor+"#"+successor_1+"#"+predeccessor2;
						//Log.v(TAG, "Retreive back when alive" + msg_request + "ALIVE" + messages);

						 out = socket.getOutputStream();
						 d = new DataOutputStream(out);
						d.writeUTF(msg_request);

						 in = socket.getInputStream();
						din = new DataInputStream(in);

						ack = din.readUTF();
						Log.d(TAG, "Replying back from predecessor 1 to alive avd" + ack);

						//if (ack.equals("OK") || ack.equals(null))
						//	socket.close();
						//else {
						socket.close();
						String temp[] = ack.split("#");
						String keys[] = temp[2].split(",");
						String values[] = temp[3].split(",");
						for (int j = 0; j < keys.length; j++) {
							cursor.addRow(new Object[]{keys[j], values[j]});

						}
						out.close();
						d.close();
						in.close();
						din.close();

					}catch (IOException e){
						e.printStackTrace();
					}

					try{
						String pred_3;

						int index=store.indexOf(predeccessor2);

						if(index==0)
							pred_3=store.get(4);
						else
							pred_3=store.get(index-1);
						Log.d(TAG,"Index of predecessor 2"+index+" "+pred_3);

						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(predeccessor2) * 2);

						String msg_request = portStr+"#"+type_of_work[12]+"#"+predeccessor+"#"+successor_1+"#"+predeccessor2+"#"+pred_3;
						//Log.v(TAG, "Retreive back when alive" + msg_request + "ALIVE" + messages);


						 out = socket.getOutputStream();
						 d = new DataOutputStream(out);
						d.writeUTF(msg_request);

						in = socket.getInputStream();
						din = new DataInputStream(in);

						ack = din.readUTF();
						Log.d(TAG, "Replying back from predecessor 2 to alive avd" + ack);

						//if (ack.equals("OK") || ack.equals(null))
						//	socket.close();
						//else {
						socket.close();
						String temp[] = ack.split("#");
						String keys[] = temp[2].split(",");
						String values[] = temp[3].split(",");
						for (int j = 0; j < keys.length; j++) {
							cursor.addRow(new Object[]{keys[j], values[j]});

						}
						Log.d(TAG,"Cursor"+cursor.getCount());
						out.close();
						d.close();
						in.close();
						din.close();

					}catch (IOException e){
						e.printStackTrace();
					}

				}

			}
			catch (Exception e) {
			Log.e(TAG, "ClientTask UnknownHostException");
			}
		return null;
		}


		}

	}

