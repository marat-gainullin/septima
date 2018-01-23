import com.septima.application.endpoint.SqlEntitiesDataEndPoint;

import javax.servlet.annotation.WebServlet;

@WebServlet(asyncSupported = true, urlPatterns = "/public/*")
public class PublicPoints extends SqlEntitiesDataEndPoint {
}
