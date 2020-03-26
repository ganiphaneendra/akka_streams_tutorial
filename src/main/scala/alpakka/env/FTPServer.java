package alpakka.env;

//import org.apache.ftpserver.FtpServer;
//import org.apache.ftpserver.FtpServerFactory;
//import org.apache.ftpserver.filesystem.nativefs.NativeFileSystemFactory;
//import org.apache.ftpserver.ftplet.*;
//import org.apache.ftpserver.listener.ListenerFactory;
//import org.apache.ftpserver.usermanager.PasswordEncryptor;
//import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
//import org.apache.ftpserver.usermanager.impl.BaseUser;
//import org.apache.ftpserver.usermanager.impl.WritePermission;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;

/**
 * Dummy FTPServer for local testing
 * Taken from:
 * https://stackoverflow.com/questions/8969097/writing-a-java-ftp-server
 *
 */
//public class FTPServer {
//	private static final Logger logger = LoggerFactory.getLogger(FTPServer.class);
//
//	public static void main(String[] args) {
//		FtpServerFactory serverFactory = new FtpServerFactory();
//		ListenerFactory factory = new ListenerFactory();
//		factory.setPort(9999);
//		serverFactory.addListener("default", factory.createListener());
//
//		NativeFileSystemFactory fileSystemFactory = new NativeFileSystemFactory();
//		fileSystemFactory.setCreateHome(true);
//		serverFactory.setFileSystem(fileSystemFactory);
//
//		PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
//		userManagerFactory.setFile(new File("data/myusers.properties")); //where to read user list
//		userManagerFactory.setPasswordEncryptor(new PasswordEncryptor() {//clear-text passwords
//
//			@Override
//			public String encrypt(String password) {
//				return password;
//			}
//
//			@Override
//			public boolean matches(String passwordToCheck, String storedPassword) {
//				return passwordToCheck.equals(storedPassword);
//			}
//		});
//		//Let's add a user, since our myusers.properties files is empty on our first test run
//		BaseUser user = new BaseUser();
//		user.setName("sftpuser");
//		user.setPassword("sftpuser");
//		user.setHomeDirectory("data");
//		List<Authority> authorities = new ArrayList<Authority>();
//		authorities.add(new WritePermission());
//		user.setAuthorities(authorities);
//		UserManager um = userManagerFactory.createUserManager();
//		try {
//			um.save(user);//Save the user to the user list on the filesystem
//		} catch (FtpException e1) {
//			//Deal with exception as you need
//		}
//		serverFactory.setUserManager(um);
//		Map<String, Ftplet> m = new HashMap<String, Ftplet>();
//		m.put("miaFtplet", new Ftplet() {
//
//			@Override
//			public void init(FtpletContext ftpletContext) throws FtpException {
//				//logger.info("init");
//				//logger.info("Thread #" + Thread.currentThread().getId());
//			}
//
//			@Override
//			public void destroy() {
//				//logger.info("destroy");
//				//logger.info("Thread #" + Thread.currentThread().getId());
//			}
//
//			@Override
//			public FtpletResult beforeCommand(FtpSession session, FtpRequest request) throws FtpException, IOException {
//				logger.info("beforeCommand " + session.getUserArgument() + " : " + session.toString() + " | " + request.getArgument() + " : " + request.getCommand() + " : " + request.getRequestLine());
//				//logger.info("Thread #" + Thread.currentThread().getId());
//
//				//do something
//				return FtpletResult.DEFAULT;//...or return accordingly
//			}
//
//			@Override
//			public FtpletResult afterCommand(FtpSession session, FtpRequest request, FtpReply reply) throws FtpException, IOException {
//				//logger.info("afterCommand " + session.getUserArgument() + " : " + session.toString() + " | " + request.getArgument() + " : " + request.getCommand() + " : " + request.getRequestLine() + " | " + reply.getMessage() + " : " + reply.toString());
//				//logger.info("Thread #" + Thread.currentThread().getId());
//
//				//do something
//				return FtpletResult.DEFAULT;//...or return accordingly
//			}
//
//			@Override
//			public FtpletResult onConnect(FtpSession session) throws FtpException, IOException {
//				//logger.info("onConnect " + session.getUserArgument() + " : " + session.toString());
//				//logger.info("Thread #" + Thread.currentThread().getId());
//
//				//do something
//				return FtpletResult.DEFAULT;//...or return accordingly
//			}
//
//			@Override
//			public FtpletResult onDisconnect(FtpSession session) throws FtpException, IOException {
//				logger.info("onDisconnect " + session.getUserArgument() + " : " + session.toString());
//				//logger.info("Thread #" + Thread.currentThread().getId());
//
//				//do something
//				return FtpletResult.DEFAULT;//...or return accordingly
//			}
//		});
//		serverFactory.setFtplets(m);
//		//Map<String, Ftplet> mappa = serverFactory.getFtplets();
//		//logger.info(mappa.size());
//		//logger.info("Thread #" + Thread.currentThread().getId());
//		//logger.info(mappa.toString());
//		FtpServer server = serverFactory.createServer();
//		try {
//			server.start();
//		} catch (FtpException ex) {
//			//Deal with exception as you need
//		}
//	}
//}
