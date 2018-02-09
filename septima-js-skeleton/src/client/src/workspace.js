import AnchorsPane from 'kenga-containers/anchors-pane';
import ColumnNode from 'kenga-grid/columns/column-node';
import MarkerServiceNode from 'kenga-grid/columns/nodes/marker-service-node';
import Grid from 'kenga-grid/grid';

class KengaWidgets {
    constructor () {
        const grdPets = new Grid();
        this.grdPets = grdPets;
        const colService = new MarkerServiceNode();
        this.colService = colService;
        const colName = new ColumnNode();
        this.colName = colName;
        const colType = new ColumnNode();
        this.colType = colType;
        const colDateOfBirth = new ColumnNode();
        this.colDateOfBirth = colDateOfBirth;
        const surface = new AnchorsPane();
        this.surface = surface;
        surface.add(grdPets);
        grdPets.addColumnNode(colService);
        grdPets.addColumnNode(colName);
        grdPets.addColumnNode(colType);
        grdPets.addColumnNode(colDateOfBirth);
        {
            grdPets.element.style.left = '47px';
            grdPets.element.style.width = '402px';
            grdPets.element.style.top = '36px';
            grdPets.element.style.height = '338px';
        }
        {

        }
        {
            colName.title = 'Name';
        }
        {
            colType.title = 'Type';
            colType.width = 92;
        }
        {
            colDateOfBirth.title = 'Date of birth';
            colDateOfBirth.width = 211;
        }
        {
            surface.element.style.width = '500px';
            surface.element.style.height = '500px';
        }
    }
}
export default KengaWidgets;