package mj.mega.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

public class MjTask {

	public MjTask() {
		Main.logger = Logger.getLogger(getClass());
	}

	String ZipFileName = null;

	public void Zip() throws Exception {

		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm_ss");
		LocalDateTime now = LocalDateTime.now();
		ZipFileName = Main.prop.getProperty("DbPrefix") + "_" + dtf.format(now) + ".zip";

		List<String> srcFiles = Arrays.asList(Main.prop.getProperty("ToFolder") + "/dmp.dmp",
				Main.prop.getProperty("ToFolder") + "/dmp.log");
		FileOutputStream fos = new FileOutputStream(Main.prop.getProperty("ToFolder") + "/" + ZipFileName);
		ZipOutputStream zipOut = new ZipOutputStream(fos);
		zipOut.setLevel(Deflater.BEST_COMPRESSION);
		for (String srcFile : srcFiles) {
			File fileToZip = new File(srcFile);
			FileInputStream fis = new FileInputStream(fileToZip);
			ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
			zipOut.putNextEntry(zipEntry);

			byte[] bytes = new byte[1024];
			int length;
			while ((length = fis.read(bytes)) >= 0) {
				zipOut.write(bytes, 0, length);
			}
			fis.close();
		}
		zipOut.close();
		fos.close();
	}

	// public static Properties prop = new Properties();

	public static void main(String[] args) throws Exception {
		DOMConfigurator.configure(Main.class.getResource("/log4j.xml"));

		System.out.println(System.getenv("IbankFiz") + "/config.properties");
		// load a properties file
		try {
			InputStream input = new FileInputStream(System.getenv("IbankFiz") + "/config.properties");
			Main.prop.load(input);
		} catch (Exception e) {
			Main.logger.error(getStackTrace(e));
			System.exit(0);
		}
		MjTask mytask = new MjTask();
		mytask.Run();
	}

	public static String getStackTrace(final Throwable throwable) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw, true);
		throwable.printStackTrace(pw);
		return sw.getBuffer().toString();
	}

	public void Run() {
		try {
			// properties
			String hostName = Main.prop.getProperty("SfpRemoteHost");
			String username = Main.prop.getProperty("SfpUserName");
			String password = Main.prop.getProperty("SfpPassword");
			String localFilePath = Main.prop.getProperty("ToFolder");
			String remoteFilePath = Main.prop.getProperty("FromFolder");

			// delete local
			FileUtils.cleanDirectory(new File(localFilePath));
			// delete dump file
			delete(hostName, username, password, remoteFilePath + "/dmp.dmp");
			delete(hostName, username, password, remoteFilePath + "/dmp.log");
			// execute export to dump
			Expdp();
			// download
			download(hostName, username, password, localFilePath + "/dmp.dmp", remoteFilePath + "/dmp.dmp");
			download(hostName, username, password, localFilePath + "/dmp.log", remoteFilePath + "/dmp.log");
			// Zip
			Zip();
			// move
			Files.move(Paths.get(Main.prop.getProperty("ToFolder") + "/" + ZipFileName),
					Paths.get(Main.prop.getProperty("MegaFolder") + "/" + ZipFileName),
					StandardCopyOption.REPLACE_EXISTING);
		} catch (Exception e) {
			Main.logger.error(Main.getStackTrace(e));
		}
	}

	public void Expdp() throws Exception {
		String line;
		ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/c", System.getenv("IbankFiz") + "/Expdp.bat");
		builder.redirectErrorStream(true);
		Process p = builder.start();
		BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), "Cp866"));
		while ((line = r.readLine()) != null) {
			Main.logger.info(line);
		}
	}

	public static void upload(String hostName, String username, String password, String localFilePath,
			String remoteFilePath) {
		File file = new File(localFilePath);
		if (!file.exists())
			throw new RuntimeException("Error. Local file not found");
		StandardFileSystemManager manager = new StandardFileSystemManager();
		try {
			manager.init();
			// Create local file object
			FileObject localFile = manager.resolveFile(file.getAbsolutePath());
			// Create remote file object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, username, password, remoteFilePath), createDefaultOptions());
			// Copy local file to sftp server
			remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);

			System.out.println("File upload success");
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}

	public static boolean move(String hostName, String username, String password, String remoteSrcFilePath,
			String remoteDestFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();
		try {
			manager.init();
			// Create remote object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, username, password, remoteSrcFilePath), createDefaultOptions());
			FileObject remoteDestFile = manager.resolveFile(
					createConnectionString(hostName, username, password, remoteDestFilePath), createDefaultOptions());

			if (remoteFile.exists()) {
				remoteFile.moveTo(remoteDestFile);
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}

	public static void download(String hostName, String username, String password, String localFilePath,
			String remoteFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();
		try {
			manager.init();
			// downloadFilePath
			FileObject localFile = manager.resolveFile(localFilePath);
			// Create remote file object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, username, password, remoteFilePath), createDefaultOptions());
			// Copy local file to sftp server
			localFile.copyFrom(remoteFile, Selectors.SELECT_SELF);
			// close
			localFile.close();
			remoteFile.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}

	public static void delete(String hostName, String username, String password, String remoteFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();
		try {
			manager.init();
			// Create remote object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, username, password, remoteFilePath), createDefaultOptions());
			if (remoteFile.exists()) {
				remoteFile.delete();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}

	// Check remote file is exist function:
	public static boolean exist(String hostName, String username, String password, String remoteFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();
		try {
			manager.init();
			// Create remote object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, username, password, remoteFilePath), createDefaultOptions());
			return remoteFile.exists();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}

	public static String createConnectionString(String hostName, String username, String password,
			String remoteFilePath) {
		return "sftp://" + username + ":" + password + "@" + hostName + "/" + remoteFilePath;
	}

	@SuppressWarnings("deprecation")
	public static FileSystemOptions createDefaultOptions() throws FileSystemException {
		// Create SFTP options
		FileSystemOptions opts = new FileSystemOptions();
		// SSH Key checking
		SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");
		// Root directory set to user home
		SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, false);
		// Timeout is count by Milliseconds
		SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);
		return opts;
	}

}