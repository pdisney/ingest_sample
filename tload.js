'use strict';
const pg = require('postgres');
const fs = require('fs');
const path = require("path");
const manifest_ds = require('./sample_data/glbl3a_cdw/manifest.json');






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
            for (let i = 0; i < result.length; i++) {
                edge_types[result[i].name.replace(/ /g, '_')] = result[i].id;
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
            for (let i = 0; i < result.length; i++) {
                node_types[result[i].name.replace(' ', '_')] = result[i].id;
            }
            return node_types;
        } catch (e) {
            console.log(e);
        }
    }

    const node_types = await getNodeTypes();
    const edge_types = await getEdgeTypes();

  
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


    const ds_id = await getNode('name', manifest_ds.name);
    console.log(`Dataset id: ${ds_id}`);


    let manifest = require(`./sample_data/${process.env.SAMPLEPATH}/manifest.json`);

    let tbl_id = await getNode('title', manifest.title);
    if (!tbl_id) {
        let metadata_tbl = {};
        metadata_tbl.source_system = manifest.source_system;
        metadata_tbl.title = manifest.title;
        metadata_tbl.description = manifest.description;
        metadata_tbl.image_url = "";

        tbl_id = await insertNode('table', metadata_tbl);
        await insertEdge(ds_id, 'dataset_contains', tbl_id);
        await insertEdge(tbl_id, 'table_of', ds_id);
        await updateVisibility(tbl_id, manifest.authorizations);

        for (let x = 0; x < manifest.owner.length; x++) {
            const owner = manifest.owner[x];
            let owner_id = await getNode('Uid', owner.Uid);
            if (!owner_id) {
                owner_id = await insertNode('user', owner[x]);
            }
            await insertEdge(owner_id, 'owns', tbl_id);

            console.log(`Owns edge created between ${owner_id} and ${tbl_id}`);
        }
    }

    // let row_ids = [];
    let samples = require(`./sample_data/${process.env.SAMPLEPATH}/${process.env.SAMPLEFILE}`);
    for (let k = 0; k < samples.length; k++) {
        const keys = Object.keys(samples[0]);
        console.log(`Row ${k} of ${samples.length} `);
        try {
            const row = samples[k];
            if (row) {
                const rowid = await insertNode('row', row);
                await updateVisibility(rowid, manifest.authorizations);
                console.log(`Row node created with id: ${rowid}`);
                await insertEdge(ds_id, 'dataset_contains', rowid);
                await insertEdge(tbl_id, 'table_contains', rowid);
                await insertEdge(rowid, 'row_of', tbl_id);
                await insertEdge(rowid, 'row_of', ds_id);
                for (let l = 0; l < keys.length; l++) {
                    const cellkey = keys[l];
                    const cell = {};
                    cell[cellkey] = row[cellkey];
                    const cellid = await insertNode('cell', cell);
                    await insertEdge(ds_id, 'dataset_contains', cellid);
                    await insertEdge(tbl_id, 'table_contains', cellid);
                    await insertEdge(rowid, 'row_contains', cellid);
                    await insertEdge(cellid, 'cell_of', ds_id);
                    await insertEdge(cellid, 'cell_of', tbl_id);
                    await insertEdge(cellid, 'cell_of', rowid);
                    await updateVisibility(cellid, manifest.authorizations);
                }
            }
        } catch (e) {
            console.log(e);
            break;
        }

    }


}

main();