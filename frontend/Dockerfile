# Dockerfile très simple pour Angular
FROM nginx:alpine

# Copier les fichiers HTML statiques (on va les créer après)
COPY . /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]