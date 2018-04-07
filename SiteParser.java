package biz.redfrigate.merchantservices.web.main;

import au.com.bytecode.opencsv.CSVWriter;
import biz.redfrigate.merchantservices.web.servlet.InitParams;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.glassfish.jersey.servlet.ServletContainer;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.imageio.ImageIO;
import javax.ws.rs.core.Response;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static au.com.bytecode.opencsv.CSVWriter.NO_QUOTE_CHARACTER;

/**
 * Grishko
 * 01.04.2018
 */
public class SiteParser extends ServletContainer {

    private final static String siteUrlForParsing = "https://service.nalog.ru/tabak.do";
    private final static String captchaFileName = "captchaImage.jpg";
    private static String date = ""; // example = 29.03.2018 if value "" that use current date
    private static Connection connect;
    public final static String resourceDirectory = InitParams.getInstance().getServletConfig().getInitParameter("resourceDirectory");
    public final static String resultDirectory = InitParams.getInstance().getServletConfig().getInitParameter("resultDirectory");

    public void parse() throws IOException {
        deletePrevJpgAndCsvFiles();

        Connection.Response res = getResponseFromSite();
        if (res != null)
            prepareRequest(res);
    }

    public SiteParser() {
        connect = Jsoup.connect(siteUrlForParsing).data("c", "search");
        if (CookieHandler.getDefault() == null) {
            CookieManager cookieManager = new CookieManager(null, CookiePolicy.ACCEPT_ALL);
            CookieHandler.setDefault(cookieManager);
        }
    }

    public static Response.ResponseBuilder getDataFromSite(String captchaCode) {
        int num = captchaCode.indexOf("=");
        String capSubstr = captchaCode.substring(num + 1, captchaCode.length());

        if (date.equals("")) date = Utils.getCurrentDate();

        connect.data("product", "")
                .data("dt", date)
                .timeout(0)
                .data("cap", capSubstr);

        try {
            Document doc = sendRequestToSite(connect);
            if (doc != null) {
                return parseResponseFromSite(doc);
            } else
                return Response.serverError();
        } catch (IOException e) {
            e.printStackTrace();
            return Response.serverError();
        }
    }

    private static final String NAME = "Название";
    private static final String ADRESS = "Адрес";
    private static final String INN = "ИНН";
    private static final String KPP = "КПП";

    private static Response.ResponseBuilder parseResponseFromSite(Document doc) {
        try {
            File csv = new File(resultDirectory + Utils.getCsvFileName());
            CSVWriter writer = new CSVWriter(new FileWriter(csv), '|', NO_QUOTE_CHARACTER);
            //Create record
            int numRecord = 0;
            Elements resultElements = doc.select("tr");
            Map<String, String> manufacturers = null;
            for (Element headline : resultElements) {
                String s = "";
                if (!headline.attributes().get("class").equals("heading")) {
                    for (int i = 1; i <= 6; i++) {
                        if (numRecord != 0 && i == 4)
                            s += changeDateFormat(headline.child(i).text()) + "|";
                        else
                            s += headline.child(i).text() + "|";
                    }

                    if (numRecord == 0)
                        s += NAME + "|" + ADRESS + "|" + INN + "|" + KPP;
                    else if (manufacturers != null)
                        s += manufacturers.get(NAME) + "|" + manufacturers.get(ADRESS) + "|" + manufacturers.get(INN) + "|" + manufacturers.get(KPP);
                    numRecord++;
                    s.trim();
                    s.replace("</td>", "");
                    String[] record = s.split("\\|");
                    //Write the record to file
                    writer.writeNext(record);
                } else {
                    if (manufacturers != null)
                        manufacturers.clear();
                    s = headline.child(0).text();
                    manufacturers = getManufactData(s);
                }
                // System.out.println(s);
            }
            //close the writer
            writer.close();
        } catch (Exception e) {
            System.out.println(e.toString());
            return Response.serverError();
        }
        saveZipFile();


        String bucketName = InitParams.getInstance().getServletConfig().getInitParameter("bucketName");
        String directories = InitParams.getInstance().getServletConfig().getInitParameter("directoriesS3");
        String keyName = directories + Utils.getZipFileName();
        if (!bucketName.equals("") && !keyName.equals(""))
            saveToS3(bucketName, keyName);

        return Response.ok();
    }

    private static String changeDateFormat(String text) {
        try {
            LocalDate newFormatDate = LocalDate.parse(
                    text.replace("</td>", ""),
                    DateTimeFormatter.ofPattern("dd.MM.uuuu", Locale.UK)
            );
            return newFormatDate.toString();
        } catch (Exception e) {
            System.out.println(e.toString());
            return "";
        }
    }

    private static Map<String, String> getManufactData(String s) {
        Map<String, String> manufacturers = new HashMap<>();
        int innStartPos = s.indexOf(INN);
        int kppStartPos = s.indexOf(KPP);
        int adressStartPos = s.indexOf(ADRESS);
        manufacturers.put(NAME, s.substring(0, innStartPos));
        manufacturers.put(INN, s.substring(innStartPos + 4, kppStartPos).replaceAll("[ ,]", ""));
        manufacturers.put(KPP, s.substring(kppStartPos + 4, adressStartPos).replaceAll("[ ,]", ""));
        manufacturers.put(ADRESS, s.substring(adressStartPos + 6, s.length()));
        return manufacturers;
    }

    private static void saveZipFile() {
        if (new File(resultDirectory + Utils.getCsvFileName()).exists()) {
            byte[] buffer = new byte[1024];
            try {
                FileOutputStream fos = new FileOutputStream(resultDirectory + Utils.getZipFileName());
                ZipOutputStream zos = new ZipOutputStream(fos, Charset.forName("UTF-8"));
                ZipEntry ze = new ZipEntry(Utils.getCsvFileName());
                zos.putNextEntry(ze);
                FileInputStream in = new FileInputStream(resultDirectory + Utils.getCsvFileName());
                int len;
                while ((len = in.read(buffer)) > 0) {
                    zos.write(buffer, 0, len);
                }
                in.close();
                zos.closeEntry();
                //remember close it
                zos.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void saveToS3(String bucketName, String keyName) {
        String awsAccessKey = InitParams.getInstance().getServletConfig().getInitParameter("awsAccessKey");
        String awsSecretKey = InitParams.getInstance().getServletConfig().getInitParameter("awsSecretKey");
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        AmazonS3 s3client = new AmazonS3Client(awsCredentials);
        try {
            System.out.println("Uploading a new object to S3 from a file\n");
            File file = Utils.getFileFromDirectory(".zip");
            if (file != null && file.exists()) {

                s3client.putObject(new PutObjectRequest(bucketName, keyName, file));
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

    }

    private static Document sendRequestToSite(Connection connection) throws IOException {
        Connection.Response res = connection.method(Connection.Method.POST).execute();
        Document doc = res.parse();
        Elements resultsData = doc.select("[id=pnlResults]");
        if (resultsData.size() != 0)
            return doc;
        else {
            return null;
            //throw new IOException("Not result - " + doc.select("div.field-error").toString());
        }
    }

    private static Connection prepareRequest(Connection.Response res) throws IOException {
        String sessionId = res.cookie("JSESSIONID");
        if (sessionId == null) {
            CookieManager m = (CookieManager) CookieHandler.getDefault();
            CookieStore d = m.getCookieStore();
            sessionId = d.getCookies().get(0).getValue();
        }
        Document doc = res.parse();
        Elements newsHeadlines = doc.select("div.checkbox-item");
        for (Element headline : newsHeadlines) {
            connect.data(headline.childNode(1).attr("name"), headline.childNode(1).attr("value"));
        }

        getCaptcha(doc);
        connect
                .data("_multiselect_producer", "")
                .cookie("JSESSIONID", sessionId);
        return connect;
    }

    private static void getCaptcha(Document doc) {
        Elements captchaImg = doc.select("[id=capImg]");
        String capImgUrl = captchaImg.attr("src");
        try {
            saveImg(capImgUrl);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Connection.Response getResponseFromSite() {
        try {
            String sessionId = null;
            Connection.Response res = Jsoup.connect(siteUrlForParsing).method(Connection.Method.GET).execute();
            return res;
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        return null;
    }

    private static void deletePrevJpgAndCsvFiles() {
        File folder = new File(resultDirectory);
        File[] listFiles = folder.listFiles();
        if (listFiles != null)
            for (File f : listFiles)
                if (f.getName().endsWith(".jpg") || f.getName().endsWith(".csv") || f.getName().endsWith(".zip"))
                    f.delete();

    }

    private static void saveImg(String pic_href) throws IOException {
        BufferedImage image = null;
        URL url = new URL(siteUrlForParsing + "/" + pic_href);
        try {
            image = ImageIO.read(url);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (image != null) {
            String imgJpeg = resultDirectory + captchaFileName;
            File f = new File(resultDirectory);
            if (!f.exists())
                throw new IllegalArgumentException(resultDirectory +"\n" + f.getAbsolutePath());
            ImageIO.write(image, "jpg", new File(imgJpeg));
//      Desktop dt = Desktop.getDesktop();
//      dt.open(new File(imgJpeg));
        }
    }
}
