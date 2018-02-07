import Button from 'kenga-buttons/button';
import AnchorsPane from 'kenga-containers/anchors-pane';
import EmailField from 'kenga-fields/email-field';
import PasswordField from 'kenga-fields/password-field';
import Label from 'kenga-labels/label';

class KengaWidgets {
    constructor () {
        const anchorsPane = new AnchorsPane();
        this.anchorsPane = anchorsPane;
        const btnSignIn = new Button();
        this.btnSignIn = btnSignIn;
        const txtEmail = new EmailField();
        this.txtEmail = txtEmail;
        const txtPassword = new PasswordField();
        this.txtPassword = txtPassword;
        const lblLogo = new Label();
        this.lblLogo = lblLogo;
        const badCredentials = new Label();
        this.badCredentials = badCredentials;
        anchorsPane.add(lblLogo);
        anchorsPane.add(txtEmail);
        anchorsPane.add(txtPassword);
        anchorsPane.add(btnSignIn);
        anchorsPane.add(badCredentials);
        {
            anchorsPane.element.style.width = '500px';
            anchorsPane.element.style.height = '500px';
        }
        {
            btnSignIn.text = 'Sign in';
            btnSignIn.element.style.left = '336px';
            btnSignIn.element.style.width = '128px';
            btnSignIn.element.style.top = '314px';
            btnSignIn.element.style.height = '40px';
        }
        {
            txtEmail.emptyText = 'E - mail';
            txtEmail.element.style.left = '40px';
            txtEmail.element.style.width = '426px';
            txtEmail.element.style.top = '180px';
            txtEmail.element.style.height = '40px';
        }
        {
            txtPassword.emptyText = 'Password';
            txtPassword.element.style.left = '40px';
            txtPassword.element.style.width = '426px';
            txtPassword.element.style.top = '250px';
            txtPassword.element.style.height = '40px';
        }








        {
            lblLogo.text = 'Logo';
            lblLogo.element.style.left = '120px';
            lblLogo.element.style.width = '248px';
            lblLogo.element.style.top = '20px';
            lblLogo.element.style.height = '135px';
        }
        {
            badCredentials.visible = false;
            badCredentials.text = 'Invalid e-mail or password';
            badCredentials.element.style.left = '40px';
            badCredentials.element.style.width = '421px';
            badCredentials.element.style.top = '380px';
            badCredentials.element.style.height = '18px';
        }
    }
}
export default KengaWidgets;