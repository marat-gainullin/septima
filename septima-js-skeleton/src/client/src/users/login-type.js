import Button from 'kenga-buttons/button';
import AnchorsPane from 'kenga-containers/anchors-pane';
import Label from 'kenga-labels/label';

class KengaWidgets {
    constructor () {
        const surface = new AnchorsPane();
        this.surface = surface;
        const registerByEmail = new Button();
        this.registerByEmail = registerByEmail;
        const logo = new Label();
        this.logo = logo;
        const continueWithFacebook = new Button();
        this.continueWithFacebook = continueWithFacebook;
        const alreadyRegistered = new Label();
        this.alreadyRegistered = alreadyRegistered;
        const signIn = new Label();
        this.signIn = signIn;
        surface.add(logo);
        surface.add(continueWithFacebook);
        surface.add(registerByEmail);
        surface.add(alreadyRegistered);
        surface.add(signIn);
        {
            surface.element.style.width = '300px';
            surface.element.style.height = '400px';
        }
        {
            registerByEmail.text = 'E-Mail';
            registerByEmail.element.style.left = '60px';
            registerByEmail.element.style.width = '181px';
            registerByEmail.element.style.top = '250px';
            registerByEmail.element.style.height = '41px';
        }
        {
            logo.text = 'Logo';
            logo.horizontalTextPosition = 'center';
            logo.element.style.left = '60px';
            logo.element.style.width = '177px';
            logo.element.style.top = '40px';
            logo.element.style.height = '118px';
        }
        {
            continueWithFacebook.text = 'Facebook';
            continueWithFacebook.element.style.left = '60px';
            continueWithFacebook.element.style.width = '181px';
            continueWithFacebook.element.style.top = '190px';
            continueWithFacebook.element.style.height = '41px';
        }
        {
            alreadyRegistered.text = 'Already registered ?';
            alreadyRegistered.element.style.left = '60px';
            alreadyRegistered.element.style.top = '310px';
            alreadyRegistered.element.style.height = '20px';
        }
        {
            signIn.text = 'Sign in here';
            signIn.element.style.left = '60px';
            signIn.element.style.top = '340px';
            signIn.element.style.height = '20px';
        }
    }
}
export default KengaWidgets;