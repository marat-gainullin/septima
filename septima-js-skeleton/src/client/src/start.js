import LoginTypeView from './users/login-type';
import Workspace from './workspace';

if (document.body.id === 'login') {
    document.body.appendChild(new LoginTypeView().surface.element);
} else if (document.body.id === 'main') {
    document.body.appendChild(new Workspace().surface.element);
}