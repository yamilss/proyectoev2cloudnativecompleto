export const environment = {
  production: true,

  azure: {
    clientId: 'df1b7a79-8fd6-436b-8eca-c123b29f0457',
    tenantId: '3070fad0-b050-48d9-b664-a1cb8ddc6e0a',

    authority: 'https://login.microsoftonline.com/3070fad0-b050-48d9-b664-a1cb8ddc6e0a',

    // La URL de tu sitio en la EC2 para que Microsoft te devuelva aquí
    redirectUri: 'http://localhost:4200/',

    api: {
      clientId: 'ac800c78-64d2-4043-913d-b99264309ef6',
      scope: 'api://ac800c78-64d2-4043-913d-b99264309ef6/access_as_user',

      // Apuntamos a la IP de tu EC2 con el puerto del BFF
      url: '/api',
    },
  },
};
