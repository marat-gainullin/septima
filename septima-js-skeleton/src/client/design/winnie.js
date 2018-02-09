import Invoke from 'septima-utils/invoke';
import Logger from 'septima-utils/logger';
import Model from 'winnie/model';
import widgets from 'winnie/palette/kenga';
import templates from 'winnie/templates';
import modelToEs6 from 'winnie/serial/model-to-code';
import requests from 'septima-remote/requests';

const preludeModuleSuffix = '../src/';
const designPrefix = 'design://' + preludeModuleSuffix;

function winnie(_require) {
    if (document.body.id.startsWith(designPrefix)) {
        const model = new Model();

        model.palette.add(widgets);
        model.adopts.add(templates);

        model.layout.ground.element.style.width = '100%';
        model.layout.ground.element.style.height = '100%';
        document.body.appendChild(model.layout.ground.element);

        const moduleName = document.body.id.substring(designPrefix.length);
        Invoke.later(() => {
            const split = model.layout.paletteExplorerSplit;
            split.dividerLocation = (split.height - split.dividerSize) / 2;
            model.openNatives(_require(preludeModuleSuffix + moduleName).default);
        });

        if(window.location.protocol !== 'file:') {
            model.save = () => {
                model.layout.tSave.enabled = false;
                requests.requestRpc('src', moduleName, {extension: 'js', content: modelToEs6(model)})
                    .then(r => {
                        model.layout.tSave.enabled = true;
                        Logger.info(`${moduleName} saved successfully.`);
                    })
                    .catch(e => {
                        model.layout.tSave.enabled = true;
                        Logger.severe(e);
                    });
            };
        }
    }
}

export default winnie;

