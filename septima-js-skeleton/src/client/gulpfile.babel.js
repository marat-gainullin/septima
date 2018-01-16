import gulp from 'gulp';

import yargs from 'yargs';
import gulpUtil from 'gulp-util';
import through from 'through2';
import Path from 'path';
import Capitalize from 'capitalize';
import gulpConcat from 'gulp-concat';
import gulpif from 'gulp-if';
import clean from 'gulp-clean';
import jshint from 'gulp-jshint';
import babel from 'gulp-babel';
import browserify from 'browserify';
import babelify from 'babelify';
import vinylStream from 'vinyl-source-stream';
import vinylBuffer from 'vinyl-buffer';
import uglify from 'gulp-uglify';
import sourcemaps from 'gulp-sourcemaps';

const pkg = require('./package.json');

const argv = yargs.argv;

const paths = {
    project: './',
    testSrc: 'test/',
    src: 'src/',
    build: 'build/',
    bundle: 'build/bundle/',
    lib: 'build/lib/',
    test: 'build/test/'
};

const masks = {
    scripts: '**/*.js',
    styles: '**/*.css',
    gifs: '**/*.gif',
    pngs: '**/*.png'
};

// Delete the build directory
gulp.task('clean', () => gulp.src(paths.build)
            .pipe(clean()));

// Lint JS
gulp.task('jshint', () => {
    return gulp.src(masks.scripts, {cwd: paths.src})
            .pipe(jshint())
            .pipe(jshint.reporter('default'));
});

// Process scripts
gulp.task('babel', ['clean'], () => gulp.src(masks.scripts, {cwd: paths.src})
            .pipe(babel({
                presets: ['env']
            }))
            .pipe(gulp.dest(paths.lib)));

gulp.task('code', ['jshint', 'babel'], () => {
});
// Process styles, gif images, etc
gulp.task('assets', ['clean'], () => gulp.src([masks.styles, masks.gifs, masks.pngs], {cwd: paths.src})
            .pipe(gulp.dest(paths.lib)));

function indexFrom(base) {
    const stream = through.obj((file, encoding, complete) => {
        const fileName = file.path.substring(Path.dirname(file.path).length + 1, file.path.length);
        const imported = Capitalize.words(fileName.substring(0, fileName.length - 3)).replace(/-/g, '');
        const path = file.path.substring(base.length).replace(/\\/g, '/');
        const moduleRef = '.' + path.substring(0, path.length - 3);
        const exportStmt = `export { default as ${imported} } from '${moduleRef}'`;
        file.contents = Buffer.from(exportStmt, encoding);
        stream.push(file);
        complete();
    });
    return stream;
}

function importsToIndex(imports) {
    const importStmts = imports.map((item) => {
        return `import '${item}'`;
    });
    const stream = through.obj((file, encoding, complete) => {
        if (file.isBuffer()) {
            const content = file.contents.toString(encoding);
            file.contents = Buffer.from(importStmts.join(';\n') + ';\n' + content, encoding);
            stream.push(file);
            complete();
        } else {
            stream.emit('error', new PluginError('imports-to-index', 'Only buffers are supported!'));
            return complete();
        }
    });
    return stream;
}

gulp.task('index', ['clean'], () => {
    return gulp.src([masks.scripts], {cwd: paths.src})
            .pipe(indexFrom(process.cwd() + paths.src))
            .pipe(gulpConcat(pkg.main, {newLine: ';\n'}))
            .pipe(babel({
                presets: ['env']
            }))
            .pipe(gulp.dest(paths.lib));
});

function filterPackageJson() {
    const stream = through.obj((file, encoding, complete) => {
        if ('package.json' === file.path.substring(Path.dirname(file.path).length + 1, file.path.length)) {
            var content = JSON.parse(file.contents.toString(encoding));
            delete content.devDependencies;
            delete content.scripts;
            file.contents = Buffer.from(JSON.stringify(content, null, 2), encoding);
        }
        stream.push(file);
        complete();
    });
    return stream;
}

// Copy all package related files to lib directory
gulp.task('package', ['index'], () => gulp.src([
        'LICENSE', 'package.json'], {cwd: paths.project})
            .pipe(filterPackageJson(pkg))
            .pipe(gulp.dest(paths.lib)));
gulp.task('lib', ['code', 'assets', 'package'], () => {
});

// Define the default task as a sequence of the above tasks
gulp.task('default', ['lib']);
