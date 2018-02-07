import Invoke from 'septima-utils/invoke';
import Model from 'winnie/model';
import widgets from 'winnie/palette/kenga';
import templates from 'winnie/templates';

function winnie(_require) {
    const model = new Model();

    model.palette.add(widgets);
    model.adopts.add(templates);

    model.layout.ground.element.style.width = '100%';
    model.layout.ground.element.style.height = '100%';
    document.body.appendChild(model.layout.ground.element);

    Invoke.later(() => {
        model.layout.paletteExplorerSplit.dividerLocation = (model.layout.paletteExplorerSplit.height - model.layout.paletteExplorerSplit.dividerSize) / 2;
        model.openNatives(_require(document.body.id).default);
    });
}

export default winnie;

