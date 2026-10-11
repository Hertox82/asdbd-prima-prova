# Immagine Jupyter Docker Stacks, versione bloccata per riproducibilità.
FROM quay.io/jupyter/base-notebook:x86_64-2026-04-27

# Librerie del laboratorio; utilizzare versioni fisse in requirements.txt
# quando si congela la consegna definitiva.
COPY --chown=jovyan:users requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /home/jovyan/work
