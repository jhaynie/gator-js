import nodeResolve from 'rollup-plugin-node-resolve'
import commonjs    from 'rollup-plugin-commonjs'
import babel       from 'rollup-plugin-babel'

export default {
  entry: 'src/main.js',
  dest: 'lib/bundle.js',
  format: 'cjs',
  sourceMap: true,
  plugins: [
    nodeResolve({
      jsnext:  true, // if provided in ES6
      main:    true, // if provided in CommonJS
      browser: true  // if provided for browsers
    }),
    commonjs(),
    babel({
      babelrc: false,
      presets: ['es2015-rollup'],
      exclude: 'node_modules/**'
    })
  ]
}