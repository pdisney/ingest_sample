'use strict';
const pg = require('postgres');
const fs = require('fs');
const path = require("path");



const main = async () => {

    if (process.env.NODE_ENV !== 'production') {
        const conf = { "path": __dirname + '/.env' };
        require('dotenv').config(conf);
    }

    const connection = pg(process.env.CONNECTION);
    const getEdgeTypes = async () => {
        try {
            let result = await connection`
                select name, id from public.edge_type;`
            let edge_types = {};
            for(let i = 0; i < result.length; i++){
                edge_types[result[i].name.replace(/ /g,'_')] = result[i].id;
            }
            return edge_types;
        } catch (e) {
            console.log(e);
        }
    }
    const getNodeTypes = async (type) => {
        try {
            let result = await connection`
            select name, id from public.node_type;`
            let node_types = {};
            for(let i = 0; i < result.length; i++){
                node_types[result[i].name.replace(' ','_')] = result[i].id;
            }
        return node_types;
        } catch (e) {
            console.log(e);
        }
    }

    const node_types = await getNodeTypes();
    const edge_types = await getEdgeTypes();


    await connection`DELETE FROM public.edge;`;
    await connection`DELETE FROM public.node;`;

    const insertNode = async (type, entity) => {
        try {
            let result = await connection`
                 INSERT INTO public.node (type_id, entity) VALUES (${node_types[type]},${entity}) RETURNING id;`
            return result[0].id;
        } catch (e) {
            console.log(e);
        }
    }
    const insertEdge = async (src_id, edge_type, snk_id) => {
        try {
            //  console.log(nodesrc_id, nodesnk_id, edge_type, edge_types[edge_type])
            await connection`INSERT INTO public.edge (src_id, snk_id, type_id) VALUES (${src_id},${snk_id},${edge_types[edge_type]});`
        } catch (e) {
            console.log(e);
        }
    }

    const updateVisibility = async (id, visibility) => {
        try {
            await connection`UPDATE public.node SET visibility=${visibility} WHERE id = ${id};`
        } catch (e) {
            console.log(e);
        }
    }
    const getNode = async (key, value) => {
        try {
            let result = await connection`SELECT id FROM public.node WHERE entity->>${key} = ${value};`
            console.log(result);
            return result[0].id;
        } catch (e) {
            console.log(e);
        }
    }


    console.log(node_types["dataset"]);
    let metadata_ds = {};
    const manifest_ds = require('./sample_data/glbl3a_cdw/manifest.json');
    metadata_ds.source_system = manifest_ds.source_system;
    metadata_ds.source_system_refresh = manifest_ds.source_system_refresh;
    metadata_ds.fuze_refresh = manifest_ds.fuze_refresh;
    metadata_ds.title = manifest_ds.title;
    metadata_ds.name = manifest_ds.name;
    metadata_ds.description = manifest_ds.description;
    metadata_ds.image_url = "";

    /**
     * Dataset node creation;
     */
    let ds_id = await getNode('name', manifest_ds.name);

    if(!ds_id){
        ds_id = await insertNode('dataset', metadata_ds);
        console.log(`Dataset node created with id: ${ds_id}`);
    }

    console.log(`Dataset node created with id: ${ds_id}`);
    let ds_catalog_id = null;
    for (let i = 0; i < manifest_ds.authorizations.length; i++) {
        await updateVisibility(ds_id, manifest_ds.authorizations[i]);
    }
    for (let i = 0; i < manifest_ds.owner.length; i++) {
        const owner = manifest_ds.owner[i];
        let owner_id = await getNode('Uid', owner.Uid);
        if (!owner_id) {
            owner_id = await insertNode('user',owner);
        }
        console.log(`User node created with id: ${owner_id}`);
        await insertEdge(owner_id, 'owns', ds_id);
        console.log(`Owns edge created between ${owner_id} and ${ds_id}`);
    }
    let catalog_keys = Object.keys(manifest_ds.catalog);

    for (let i = 0; i < catalog_keys.length; i++) {
        const catalog = manifest_ds.catalog[catalog_keys[i]];
        ds_catalog_id = await insertNode('data_catalog', catalog);
        await insertEdge(ds_catalog_id, 'data_catalog_of', ds_id);
    }


    console.log("ingestion complete");
    process.exit(0);

}

main();

