
export default {
  basePath: 'https://bateyss.github.io/techstoresv',
  allowedHosts: [],
  supportedLocales: {
  "en-US": ""
},
  entryPoints: {
    '': () => import('./main.server.mjs')
  },
};
