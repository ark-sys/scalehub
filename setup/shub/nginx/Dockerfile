FROM nginx:alpine3.21

ADD conf.d/http /etc/nginx/conf.d/
ADD conf.d/upstream /etc/nginx/conf.d/

ADD index.html /usr/share/nginx/html/

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]