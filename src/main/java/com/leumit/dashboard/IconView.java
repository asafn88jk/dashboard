package com.leumit.dashboard;

import org.primefaces.model.TreeNode;


import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

@Named("treeIconView")
@RequestScoped
public class IconView {

    private TreeNode root;

    @Inject
    private DocumentService service;

    @PostConstruct
    public void init() {
        root = service.createDocuments();
    }

    public void setService(DocumentService service) {
        this.service = service;
    }

    public TreeNode getRoot() {
        return root;
    }
}