import LoginView from './users/login';
import LoginTypeView from './users/login-type';

if(document.body.id === 'login'){
    document.body.appendChild(new LoginTypeView().surface.element);
} else if (document.body.id === 'login-failed') {
    const view = new LoginView();
    view.badCredentials.visible = true;
    document.body.appendChild(view.surface.element);
} else if(document.body.id === 'main') {
    document.body.appendChild(new MainView().element);
}