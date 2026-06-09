
export default {
  bootstrap: () => import('./main.server.mjs').then(m => m.default),
  inlineCriticalCss: true,
  baseHref: 'https://bateyss.github.io/techstoresv/',
  locale: undefined,
  routes: [
  {
    "renderMode": 2,
    "redirectTo": "/techstoresv/menu",
    "route": "/techstoresv"
  },
  {
    "renderMode": 2,
    "redirectTo": "/techstoresv/menu/login",
    "route": "/techstoresv/menu"
  },
  {
    "renderMode": 2,
    "route": "/techstoresv/menu/login"
  },
  {
    "renderMode": 2,
    "route": "/techstoresv/menu/productos"
  },
  {
    "renderMode": 2,
    "route": "/techstoresv/menu/carrito"
  }
],
  entryPointToBrowserMapping: undefined,
  assets: {
    'index.csr.html': {size: 26650, hash: '3d390b970e19c7bb46b8a10c5ca50af4be33b3862df9f0100162f4f4411a52c4', text: () => import('./assets-chunks/index_csr_html.mjs').then(m => m.default)},
    'index.server.html': {size: 17193, hash: '802e203925f4d1eda80cf48f12ce9c243844839d508e99fbff2d76662e0eabe1', text: () => import('./assets-chunks/index_server_html.mjs').then(m => m.default)},
    'menu/carrito/index.html': {size: 112382, hash: '41ff306dec82fa5dc73b7e3b46ce5294a59b06398e328c59d5032d41a037aecb', text: () => import('./assets-chunks/menu_carrito_index_html.mjs').then(m => m.default)},
    'menu/productos/index.html': {size: 128377, hash: 'bffd9f7c7fd36586d5176928c91ac22f3b06a17fa63b5cca1a02e0bbed6fee95', text: () => import('./assets-chunks/menu_productos_index_html.mjs').then(m => m.default)},
    'menu/login/index.html': {size: 152634, hash: '8ee656a04c2c537ba5e1ebe896ad5eb06bdd52b370f06fd923abdf687b0a7944', text: () => import('./assets-chunks/menu_login_index_html.mjs').then(m => m.default)},
    'styles-OLG4ROI3.css': {size: 11959, hash: 'OrqZl7FtrZI', text: () => import('./assets-chunks/styles-OLG4ROI3_css.mjs').then(m => m.default)}
  },
};
