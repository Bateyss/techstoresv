import { MatDialogConfig } from "@angular/material/dialog";
import { MatSnackBar, MatSnackBarConfig } from "@angular/material/snack-bar";

export class Utils {
    static snackBarConfig(): MatSnackBarConfig {
        let snackConfig: MatSnackBarConfig = new MatSnackBarConfig();
        snackConfig.duration = 12000;
        snackConfig.panelClass = ['btn', 'btn-outline-dark'];
        snackConfig.verticalPosition = 'top';
        snackConfig.horizontalPosition = 'center';
        return snackConfig;
    }

    static openSnackBar(message: string, action: string, snackBar: MatSnackBar) {
        let snackConfig = Utils.snackBarConfig();
        snackBar.open(message, action, snackConfig);
    }

    static getMatDialogConf(): MatDialogConfig {
        const dialogConfig = new MatDialogConfig();
        dialogConfig.disableClose = false;
        dialogConfig.autoFocus = true;
        dialogConfig.exitAnimationDuration = '1000ms';
        dialogConfig.enterAnimationDuration = '1000ms';
        return dialogConfig;
    }

    static async generateSHA256(txt: string): Promise<string> {
        var encoderr: TextEncoder = new TextEncoder();
        const txtBuffer = encoderr.encode(txt);
        const hashBuffer = await crypto.subtle.digest('SHA-256', txtBuffer);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    }

    static isSha256(txt: string): boolean {
        const sha256Regex = /^[0-9a-fA-F]{64}$/;
        return sha256Regex.test(txt);
    }
}
