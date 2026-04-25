FROM node:20-alpine
RUN apk add --no-cache openssl

EXPOSE 3000

WORKDIR /app

ENV NODE_ENV=production

COPY package.json package-lock.json* .npmrc ./

# Install all deps (incl. dev) so the build has access to remix tooling.
RUN npm ci --include=dev

COPY . .

RUN npx prisma generate
RUN npm run build

# Strip dev deps + Shopify CLI after build to slim the production image.
RUN npm prune --omit=dev && npm remove @shopify/cli && npm cache clean --force

CMD ["npm", "run", "docker-start"]
