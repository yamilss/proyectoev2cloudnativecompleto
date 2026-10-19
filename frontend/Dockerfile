# Etapa 1: Compilar el proyecto Angular
FROM node:22-alpine AS build
WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build -- --configuration production

# Etapa 2: Servidor Web Nginx
FROM nginx:alpine

# Copiar la configuración personalizada de Nginx
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copiar los archivos compilados a una carpeta temporal
COPY --from=build /app/dist /tmp/dist

# Limpiar Nginx y mover el contenido real de la carpeta 'browser' a la raíz
RUN rm -rf /usr/share/nginx/html/* && \
    cp -r /tmp/dist/*/browser/* /usr/share/nginx/html/ 2>/dev/null || cp -r /tmp/dist/* /usr/share/nginx/html/ && \
    rm -rf /tmp/dist

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
