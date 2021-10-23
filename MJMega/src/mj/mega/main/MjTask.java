package mj.mega.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
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

public class MjTask {

	public MjTask() {
		Main.logger = Logger.getLogger(getClass());
	}

	String ZipFileName = null;

	public void Zip() throws Exception {

		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm_ss");
		LocalDateTime now = LocalDateTime.now();
		ZipFileName = dtf.format(now) + ".zip";

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

	/**
	 * Выполнить формирование дампа
	 * 
	 * @param filename
	 * @throws Exception
	 */
	public void Expdp() throws Exception {
		String line;
		ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/c", System.getenv("MegaPath") + "/Expdp.bat");
		builder.redirectErrorStream(true);
		Process p = builder.start();
		BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), "Cp866"));
		while ((line = r.readLine()) != null) {
			Main.logger.info(line);
		}
	}

	/**
	 * Method to upload a file in Remote server
	 * 
	 * @param hostName       HostName of the server
	 * @param username       UserName to login
	 * @param password       Password to login
	 * @param localFilePath  LocalFilePath. Should contain the entire local file
	 *                       path - Directory and Filename with \\ as separator
	 * @param remoteFilePath remoteFilePath. Should contain the entire remote file
	 *                       path - Directory and Filename with / as separator
	 */
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
			/*
			 * use createDefaultOptions() in place of fsOptions for all default options -
			 * Ashok.
			 */

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
				;
				System.out.println("Move remote file success");
				return true;
			} else {
				System.out.println("Source file doesn't exist");
				return false;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}

	/**
	 * Method to download the file from remote server location
	 * 
	 * @param hostName       HostName of the server
	 * @param username       UserName to login
	 * @param password       Password to login
	 * @param localFilePath  LocalFilePath. Should contain the entire local file
	 *                       path - Directory and Filename with \\ as separator
	 * @param remoteFilePath remoteFilePath. Should contain the entire remote file
	 *                       path - Directory and Filename with / as separator
	 */
	public static void download(String hostName, String username, String password, String localFilePath,
			String remoteFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Append _downlaod_from_sftp to the given file name.
			// String downloadFilePath = localFilePath.substring(0,
			// localFilePath.lastIndexOf(".")) + "_downlaod_from_sftp" +
			// localFilePath.substring(localFilePath.lastIndexOf("."),
			// localFilePath.length());

			// Create local file object. Change location if necessary for new
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

	/**
	 * Method to delete the specified file from the remote system
	 * 
	 * @param hostName       HostName of the server
	 * @param username       UserName to login
	 * @param password       Password to login
	 * @param localFilePath  LocalFilePath. Should contain the entire local file
	 *                       path - Directory and Filename with \\ as separator
	 * @param remoteFilePath remoteFilePath. Should contain the entire remote file
	 *                       path - Directory and Filename with / as separator
	 */
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
	/**
	 * Method to check if the remote file exists in the specified remote location
	 * 
	 * @param hostName       HostName of the server
	 * @param username       UserName to login
	 * @param password       Password to login
	 * @param remoteFilePath remoteFilePath. Should contain the entire remote file
	 *                       path - Directory and Filename with / as separator
	 * @return Returns if the file exists in the specified remote location
	 */
	public static boolean exist(String hostName, String username, String password, String remoteFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Create remote object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, username, password, remoteFilePath), createDefaultOptions());

			System.out.println("File exist: " + remoteFile.exists());

			return remoteFile.exists();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}

	/**
	 * Generates SFTP URL connection String
	 * 
	 * @param hostName       HostName of the server
	 * @param username       UserName to login
	 * @param password       Password to login
	 * @param remoteFilePath remoteFilePath. Should contain the entire remote file
	 *                       path - Directory and Filename with / as separator
	 * @return concatenated SFTP URL string
	 */
	public static String createConnectionString(String hostName, String username, String password,
			String remoteFilePath) {
		return "sftp://" + username + ":" + password + "@" + hostName + "/" + remoteFilePath;
	}

	/**
	 * Method to setup default SFTP config
	 * 
	 * @return the FileSystemOptions object containing the specified configuration
	 *         options
	 * @throws FileSystemException
	 */
	@SuppressWarnings("deprecation")
	public static FileSystemOptions createDefaultOptions() throws FileSystemException {
		// Create SFTP options
		FileSystemOptions opts = new FileSystemOptions();

		// SSH Key checking
		SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");

		/*
		 * Using the following line will cause VFS to choose File System's Root as VFS's
		 * root. If I wanted to use User's home as VFS's root then set 2nd method
		 * parameter to "true"
		 */
		// Root directory set to user home
		SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, false);

		// Timeout is count by Milliseconds
		SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);

		return opts;
	}

}